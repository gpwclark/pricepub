(ns pricepub.subscribe
  (:require [pricepub.socket :as socket]
            [clojure.data.json :as json]
            [clojure.java.io :refer [writer reader copy output-stream]])
  (:import (java.net InetSocketAddress)
           (java.nio ByteBuffer)
           (java.nio.channels SocketChannel ServerSocketChannel)))

(defn subscribe-to
  ;; this ReAAlllY breaks the abstraction.
  ;; read-from-sock should just take connection info.
  [con-info topics]
  (let [{host :host port :port} con-info
       socket-addr (InetSocketAddress. host port)]
    (with-open [socket-chan (SocketChannel/open socket-addr)]
      (socket/send-on-sock socket-chan topics)
      (let [read-sock
            (future
              ;; 1. need ability to pass in an escape message. In theory to follow the spec
              ;; I should be : responding {"status":"OK"}, or {"status":"CLOSE"}
              (loop []
                (println "start loop")
                (println "message: " (socket/read-from-sock socket-chan))
                (println "end loop")
                (recur)))]
        (deref read-sock)))))

;;(subscribe-to {:host "localhost" :port 8787} "{\"topics\":[\"TOPIC\"]}")
