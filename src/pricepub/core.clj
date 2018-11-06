(ns pricepub.core
  (:gen-class)
  (:require [clojure.data.json :as json]
            [aleph.tcp :as tcp]
            [pricepub.socket :as socket]
            [digest :as digest]))

(def con-info {:host "127.0.0.1" :port 7878})

;;sample data to play with
;;(def sample-payload-map {:topic "TOPIC" :payload_size 13 :checksum "shalskdjlkjsl"})
;;(def sample-payload "PAYLOAD_BYTES")
;;(def sample-message {:header (json/write-str sample-payload-map) :payload sample-payload})

(defn get-message-otw-format
  "[map]
  A map representing a message with :header and :payload keys, will be
  turned into a string of the header followed by the payload."
  [message]
  (apply str (:header message) (:payload message)))

(defn get-n-messages-in-otw-format
  "[message n]
  takes a map representing the message format and creates a list of size n
  where each element is a string that can be sent otw on the tcp client.
  Mainly used in testing with the sample message."
  [message n]
  (take n (repeat (get-message-otw-format message))))

(defn send-messages
  "[con-info messages]
  takes connection info and creates tcp connection to server and sends
  the list of messages (which are strings) to the server in one go."
  [con-info messages]
  (socket/put {:impl :pricepub :con-info con-info :messages messages}))

(defn make-message
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

(defn create-messages
  "[topic payloads]
  takes a topic and a list of payloads (both are all strings)
  and turns it into a list of messages in over the wire format"
  [topic payloads]
  (let [make-message (partial make-message topic)
        messages (map make-message payloads)
        messages-otw (map get-message-otw-format messages)]
  ;;NOTE: messages are returned in order because (map) is used twice.
  messages-otw))

(defn on-topic-send-payloads
  "[topic & payloads]
  Currently (problematically might I add) a string, topic, and a set of payload strings. All payloads will be sent on the given topic to the server."
  [topic payloads]
  (let [messages (create-messages topic payloads)]
    (send-messages con-info messages)))

(def publish "pub")
(def subscribe "sub")

(defn error-incorrect-arg
  [arg]
  (throw (RuntimeException.
          (apply str "Valid first arguments are " publish " and "
                 subscribe "." "you provided: " arg "."))))

(defn -main
  [behavior topic & payloads]
  (cond
    (.equalsIgnoreCase behavior publish)
      (on-topic-send-payloads topic payloads)
    (.equalsIgnoreCase behavior subscribe)
      (socket/get-topics
      {:con-info {:host "localhost" :port 8787}
        :topic "{\"topics\":[\"TOPIC\"]}"})
      :else (error-incorrect-arg behavior)))

;; sample arguments
;;(on-topic-send-payloads "TOPIC" (list "aphid" "beachball" "curmudgeon" "dillweed"))
