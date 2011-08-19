(ns ekahau.engine
  (:require
    [ekahau.queue]
    [ekahau.engine.cmd :as cmd]
    [ekahau.engine.stream-connection])
  (:use
    [clojure.contrib.logging :only (debug error info spy)]
    clojure.set
    ekahau.util)
  (:import
    [com.ekahau.engine.sdk PositioningEngine IDeviceLocationListener DeviceLocation]))

;; Engine ;;

(defn- create-location-listener
  [handle-new-location handle-connection-closed]
  (proxy [IDeviceLocationListener] []
    (newDeviceLocation [^DeviceLocation location]
      (debug (str "IDeviceLocationListener.newDeviceLocation(" location ")"))
      (handle-new-location location))
    (connectionClosed  [reason]
      (debug (str "IDeviceLocationListener.connectionClosed(" reason ")"))
      (handle-connection-closed reason))))

(defn create-epe-sdk-engine
  [{:keys [host port user password]} callbacks]
  (doto (PositioningEngine. host port user password)
    (.addLocationListener
      (create-location-listener
        (:location-updated callbacks)
        (:connection-closed callbacks)))))

;; START Engine event listening ;;

(defn start-listening-erc-events!
  [engine event-handler-fn]
  (info "Starting to listen engine events.")
  (let [event-agent (:event-agent engine)]
    (send-off event-agent (fn [{:keys [connection] :as state}]
                            (let [listener (ekahau.engine.stream-connection/connect-new-queueing-listener connection)]
                              (future
                                (doseq [event-msg (ekahau.engine.stream-connection/open-emsg-seq listener)]
                                  (try
                                    (event-handler-fn event-msg)
                                    (catch Throwable e
                                      (error ("Error handling event: " event-msg) e)))))
                              (ekahau.engine.stream-connection/connect-streaming-engine-connection connection)
                              (assoc state :listener listener))))
    (await event-agent)
    (info "Listening engine events.")))

;; END Engine event listening ;;

(defn stop-listening-erc-events!
  [engine]
  (info "Stopping listening engine events.")
  (let [event-agent (:event-agent engine)]
    (send-off event-agent (fn [{:keys [connection listener] :as state}]
                            (try
                              (ekahau.engine.stream-connection/disconnect-queue-listener listener)
                              (ekahau.engine.stream-connection/close-streaming-engine-connection connection)
                              (catch Exception e (.printStackTrace e)))))
    (await event-agent)
    (info "Stopped listening engine events.")))
