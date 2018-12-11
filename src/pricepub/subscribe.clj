(ns pricepub.subscribe
  (:require [pricepub.socket :as socket]
            [clojure.data.json :as json]
            [clojure.java.io :refer [writer reader copy output-stream]])
  (:import (java.net InetSocketAddress)
           (java.nio ByteBuffer)
           (java.nio.channels SocketChannel ServerSocketChannel)))

;;(defn read-from
;;  [con-info]
;;  (let [socket-addr (InetSocketAddress. (:port con-info))]
;;    (with-open [ssc (ServerSocketChannel/open)]
;;      (let [ssc (.bind ssc socket-addr)
;;            remote (.InetSocketAddress
;;                      "localhost"
;;                      (.getLocalPort (.socket ssc)))
;;            ;;buf (ByteBuffer/wrap (.getBytes (first messages) "UTF-8"))
;;            ]
;;        (with-open
;;          [sock-chan (SocketChannel/open remote)]
;;          (let [configure
;;                (do
;;                  (.configureBlocking sock-chan false)
;;                  (.setSendBufferSize (.socket sock-chan) 8)
;;                  (.setReceiveBufferSize (.socket sock-chan) 8))
;;                sock-chan (.accept ssc)
;;                bb (ByteBuffer/allocateDirect (* 16 1024 1024))]
;;              (loop [continue true]
;;                (when continue
;;                  (println "sent: stuff?" ))
;;                  (recur (rest messages))))))))))

(defn encode-sub-message
  [topics]
  (json/write-str {:topics topics}))

(defn subscribe-to
  ;; this ReAAlllY breaks the abstraction.
  ;; read-from-sock should just take connection info.
  [con-info topics]
  (let
      [{:keys [host port]} con-info
       socket-addr (InetSocketAddress. host port)]
    (with-open [socket-chan (SocketChannel/open socket-addr)]
      (socket/send-on-sock socket-chan (encode-sub-message topics))
      ;;(pricepub.publish/write-to-once con-info topics)
      (let [read-sock
            (future
              (loop []
                (println "start loop")
                (println
                 "message: "
                 (socket/read-from-sock socket-chan))
                (println "end loop")
                (recur)))]
        ;;(deref read-sock 30000 nil)
        (deref read-sock)))))

;;(subscribe-to {:host "localhost" :port 8787} "{\"topics\":[\"TOPIC\"]}")
