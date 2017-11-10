package server

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"fmt"
	"net/url"

	protoactor "github.com/AsynkronIT/protoactor-go/actor"
	"github.com/fnproject/flow/actor"
	"github.com/fnproject/flow/cluster"
	"github.com/fnproject/flow/model"
	"github.com/fnproject/flow/persistence"
	"github.com/fnproject/flow/protocol"
	"github.com/fnproject/flow/query"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/opentracing/opentracing-go"
	"github.com/openzipkin/zipkin-go-opentracing"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

const (
	maxDelayStageDelay = 3600 * 1000 * 24
	maxRequestTimeout  = 1 * time.Hour
	minRequestTimeout  = 1 * time.Second

	paramGraphID   = "graphId"
	paramStageID   = "stageId"
	paramOperation = "operation"

	queryParamGraphID    = "graphId"
	queryParamFunctionID = "functionId"
)

var log = logrus.WithField("logger", "server")

func renderError(err error, c *gin.Context) {
	if gin.Mode() == gin.DebugMode {
		log.WithError(err).Error("Error occurred in request")
	}
	switch e := err.(type) {

	case model.ValidationError, *protocol.BadProtoMessage:
		c.Data(http.StatusBadRequest, "text/plain", []byte(e.Error()))
	case *Error:
		{
			c.Data(e.HTTPStatus, "text/plain", []byte(e.Message))
		}
	default:
		log.WithError(err).Error("Internal server error")
		c.Status(http.StatusInternalServerError)
	}
}


func (s *Server) handlePrometheusMetrics(c *gin.Context) {
	s.promHandler.ServeHTTP(c.Writer, c.Request)
}

func setTracer(ownURL string, zipkinURL string) {
	var (
		debugMode          = false
		serviceName        = "flow-service"
		serviceHostPort    = ownURL
		zipkinHTTPEndpoint = zipkinURL
		// ex: "http://zipkin:9411/api/v1/spans"
	)

	var collector zipkintracer.Collector

	// custom Zipkin collector to send tracing spans to Prometheus
	promCollector, promErr := NewPrometheusCollector()
	if promErr != nil {
		logrus.WithError(promErr).Fatalln("couldn't start Prometheus trace collector")
	}

	logger := zipkintracer.LoggerFunc(func(i ...interface{}) error { logrus.Error(i...); return nil })

	if zipkinHTTPEndpoint != "" {
		// Custom PrometheusCollector and Zipkin HTTPCollector
		httpCollector, zipErr := zipkintracer.NewHTTPCollector(zipkinHTTPEndpoint, zipkintracer.HTTPLogger(logger))
		if zipErr != nil {
			logrus.WithError(zipErr).Fatalln("couldn't start Zipkin trace collector")
		}
		collector = zipkintracer.MultiCollector{httpCollector, promCollector}
	} else {
		// Custom PrometheusCollector only
		collector = promCollector
	}

	ziptracer, err := zipkintracer.NewTracer(zipkintracer.NewRecorder(collector, debugMode, serviceHostPort, serviceName),
		zipkintracer.ClientServerSameSpan(true),
		zipkintracer.TraceID128Bit(true),
	)
	if err != nil {
		logrus.WithError(err).Fatalln("couldn't start tracer")
	}

	// wrap the Zipkin tracer in a FnTracer which will also send spans to Prometheus
	fntracer := NewFnTracer(ziptracer)

	opentracing.SetGlobalTracer(fntracer)
	logrus.WithFields(logrus.Fields{"url": zipkinHTTPEndpoint}).Info("started tracer")
}


// Server is the flow service root
type Server struct {
	Engine         *gin.Engine
	GraphManager   actor.GraphManager
	apiURL         *url.URL
	BlobStore      persistence.BlobStore
	listen         string
	requestTimeout time.Duration
	promHandler    http.Handler
}

func newEngine(clusterManager *cluster.Manager) *gin.Engine {
	engine := gin.New()
	engine.Use(gin.Logger(), gin.Recovery(), graphCreateInterceptor, clusterManager.ProxyHandler())
	return engine
}

// New creates a new server - params are injected dependencies
func New(clusterManager *cluster.Manager, manager actor.GraphManager, blobStore persistence.BlobStore, listenAddress string, maxRequestTimeout time.Duration, zipkinURL string) (*Server, error) {

	setTracer(listenAddress, zipkinURL)

	s := &Server{
		GraphManager:   manager,
		Engine:         newEngine(clusterManager),
		listen:         listenAddress,
		BlobStore:      blobStore,
		requestTimeout: maxRequestTimeout,
		promHandler:    promhttp.Handler(),
	}

	s.Engine.GET("/ping", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})
	s.Engine.GET("/metrics", s.handlePrometheusMetrics)

	createGraphApi(s, manager)

	createBlobApi(s)

	return s, nil
}


// Run starts the server
func (s *Server) Run() {
	log.WithField("listen_url", s.listen).Infof("Starting Completer server (timeout %s) ", s.requestTimeout)

	s.Engine.Run(s.listen)
}

// context handler that intercepts graph create requests, injecting a UUID parameter prior
// to forwarding to the appropriate node in the cluster
func graphCreateInterceptor(c *gin.Context) {
	if c.Request.URL.Path == "/graph" && len(c.Query(queryParamGraphID)) == 0 {
		UUID, err := uuid.NewRandom()
		if err != nil {
			c.AbortWithError(500, errors.New("Failed to generate UUID for new graph"))
			return
		}
		graphID := UUID.String()
		log.Infof("Generated new graph ID %s", graphID)

		// set the graphId query param in the original request prior to proxying
		values := c.Request.URL.Query()
		values.Add(queryParamGraphID, graphID)
		c.Request.URL.RawQuery = values.Encode()
	}
}
