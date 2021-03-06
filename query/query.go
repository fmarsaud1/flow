package query

import (
	"net/http"

	"github.com/fnproject/flow/actor"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

var log = logrus.WithField("logger", "query")

var wsupgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// WSSHandler returns a gin handler function for websocket queries
func WSSHandler(manager actor.GraphManager, w gin.ResponseWriter, r *http.Request) {
	conn, err := wsupgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Debugf("Failed to set websocket upgrade: %+v", err)
		conn.Close()
		return
	}

	wsWorker := newWorker(conn, manager)
	log.Debugf("Subscribing %v to stream", conn.RemoteAddr())
	wsWorker.Run()
}
