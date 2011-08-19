(ns ekahau.vision.VisionServlet
  (:use
    [clojure.contrib.logging :only [info error]]
    [ring.util.servlet :only [make-service-method]]
    [ekahau.vision.server :only [vision-server-loop!
                                 stop-loop!
                                 get-vision-routes-var]])
  (:gen-class
    :extends javax.servlet.http.HttpServlet
    :exposes-methods {init initSuper}
    :init init-state
    :state state))

(defn -init-state
  []
  [[] (atom nil)])

(defn -init
  ([^ekahau.vision.VisionServlet this]
    (try
      (let [m "Initing VisionServlet"]
        (.log this m)
        (info m))
      (let [loop-future (vision-server-loop! false)]
        (reset! (.state this) loop-future))
      (catch Throwable t
        (let [m "Error Initing VisionServlet"]
          (error m t)
          (.log this m t)))))
  ([^ekahau.vision.VisionServlet this context]
    (.initSuper this context)))

(defn -destroy
  [^ekahau.vision.VisionServlet this]
  (try
    (let [m "Destroying VisionServlet"]
      (.log this m)
      (info m))
    (stop-loop!)
    (deref (deref (.state this)))
    (shutdown-agents)
    (catch Throwable t
      (let [m "Error Destroying VisionServlet"]
        (error m t)
        (.log this m t)))))

(def -service
  (make-service-method
    (get-vision-routes-var)))
