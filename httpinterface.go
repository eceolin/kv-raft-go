package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type KVStoreInterface interface {
	Get(key string) (string, error)

	Set(key, value string) error

	Join(nodeID string, addr string) error
}

type JoinRequest struct {
	Id   string `json:"id"`
	Addr string `json:"addr"`
}

func startHttpServer(kv *KVStore, httpAddr string) {
	router := gin.Default()

	router.GET("/kv/:key", func(c *gin.Context) { get(c, kv) })
	router.PUT("/kv/:key", func(c *gin.Context) { set(c, kv) })
	router.POST("/join", func(c *gin.Context) { join(c, kv) })

	router.Run("localhost" + httpAddr)
}

func get(c *gin.Context, store *KVStore) {

	key := c.Param("key")

	value, error := store.Get(key)

	if error != nil {
		c.String(http.StatusBadRequest, "")
	}

	c.String(http.StatusOK, value)

}

func set(c *gin.Context, store *KVStore) {

	key := c.Param("key")
	val, er := c.GetRawData()

	if er == nil {
		error := store.Set(key, string(val[:]))

		if error != nil {
			c.String(http.StatusBadRequest, "")
		}
	} else {
		c.String(http.StatusBadRequest, "")
	}

}

func join(c *gin.Context, store *KVStore) {

	body := JoinRequest{}

	if err := c.BindJSON(&body); err != nil {
		c.AbortWithError(http.StatusBadRequest, err)
		return
	}

	store.Join(body.Id, body.Addr)

}
