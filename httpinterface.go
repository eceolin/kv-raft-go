package kvstore

type KVStoreInterface interface {
	Get(key string) (string, error)

	Set(key, value string) error

	Join(nodeID string, addr string) error
}

type kv struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

var kvs = []kv{
	{Key: "teste", Value: "value"},
}

func main() {
	router := gin.Default()
	router.PUT("/kv/", change)

	router.Run("localhost:8080")
}

func change(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, kvs)
}
