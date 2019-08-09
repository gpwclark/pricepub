(ns pricepub.publish
  (:require [pricepub.socket :as socket]
            [clojure.data.json :as json]
            [digest :as digest]))

(defn- get-message-otw-format
  "[map]
  A map representing a message with :header and :payload keys, will be
  turned into a string of the header followed by the payload."
  [message]
  (apply str (:header message) (:payload message)))

(defn- get-n-messages-in-otw-format
  "[message n]
  takes a map representing the message format and creates a list of size n
  where each element is a string that can be sent otw on the tcp client.
  Mainly used in testing with the sample message."
  [message n]
  (take n (repeat (get-message-otw-format message))))

(defn- make-message
  "[topic payload]
  takes a topic and a payload string and turns it into a map representing
  the message format."
  [topic payload]
  (let [payload-bytes (.getBytes payload)
        size (count payload-bytes)
        checksum (digest/sha-1 payload-bytes)
        payload-map (-> '{}
                        (assoc :topic topic)
                        (assoc :payload_size size)
                        (assoc :checksum checksum))]
    {:header (json/write-str payload-map) :payload payload}))

(defn- create-messages
  "[topic payloads]
  takes a topic and a list of payloads (both are all strings)
  and turns it into a list of messages in over the wire format"
  [topic payloads]
  (let [make-message (partial make-message topic)
        messages (map make-message payloads)
        messages-otw (map get-message-otw-format messages)]
    ;;NOTE: messages are returned in order because (map) is used twice.
    messages-otw))

(defn- verify-single-response
  [res]
  (-> (json/read-str res)
      (clojure.walk/keywordize-keys)
      (:status)
      (= "OK")))

(defn- send-message
  [con-info message]
  (let [res (socket/write-to-once con-info message)
        success (verify-single-response res)]
    (if success
      (println "send message succeeded.")
      (println "send message failed."))))

(defn- send-messages
  "[con-info messages]
  takes connection info and creates tcp connection to server and sends
  the list of messages (which are strings) to the server in one go."
  [con-info messages]
  (socket/write-to con-info messages))

(defn on-topic-send-payloads
  "[topic & payloads]
  Currently (problematically might I add) a string, topic, and a set of payload strings.
  All payloads will be sent on the given topic to the server."
  [con-info topic payloads]
  (let [messages (create-messages topic payloads)]
    (send-messages con-info messages)))
;;(def msg "{ \"topic\": \"TOPIC\", \"payload_size\": 7, \"checksum\": \"5116e40694ac48f654cb7b6816177e0e717237c6\"}message")
;;(send-message {:host "localhost" :port 7878} msg)

(defn- create-message
  "[topic]
  takes a topic and returns recipe for making a message on that topic"
  [topic]
  (let [make-message (partial make-message topic)]
    (comp
     (map make-message)
     (map get-message-otw-format))))

(defn- send-message-transduce
  "[con-info messages]
  takes connection info and creates tcp connection to server and sends
  the list of messages (which are strings) to the server in one go."
  [con-info]
  (let [write-to (partial socket/write-to con-info)]
    (comp (map write-to))))

(defn on-topic-send-payloads-abstr
  [con-info topic payloads]
  (let [printt (println "MEOW MEOW MEOW")
        pprintt (println "payloads: " payloads " " (type payloads) " size" (count payloads))
        send-messages (partial send-messages con-info)
        xform (comp
               (create-message topic)
               (send-message-transduce con-info))]
    ;;TODO need to aggregate responses into some sort of report?
    (transduce xform + payloads)))
