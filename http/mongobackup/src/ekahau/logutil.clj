(ns ekahau.logutil
  (:import
    [java.util Date]
    [java.util.logging Formatter ConsoleHandler Level Logger LogRecord]
    [java.text DateFormat]))

(defn create-formatter
  []
  (proxy [Formatter] []
    (format [^LogRecord record]
      (str
        (.getLoggerName record) " " (.format (DateFormat/getTimeInstance) (Date. (.getMillis record))) " " (.getMillis record) "\n"
        (.. record getLevel getName) " " (.getMessage record) "\n"))))

(defn create-console-debug-log-handler
  []
  (doto (ConsoleHandler.)
    (.setFormatter (create-formatter))
    (.setLevel (Level/ALL))))

(defn enable-debug-log-for-ns
  [name]
  (let [logger (Logger/getLogger name)]
    (doto logger
      (.setLevel Level/ALL)
      (.addHandler (create-console-debug-log-handler)))))

(defn clear-handlers!
  [name]
  (let [logger (Logger/getLogger name)]
    (doseq [handler (seq (.getHandlers logger))]
      (.removeHandler logger handler))))

;; ## Log4J

(defmulti log-level class)

(defmethod log-level org.apache.log4j.Level
  [l]
  l)

(def keyword-to-log-level
     {:all org.apache.log4j.Level/ALL
      :debug org.apache.log4j.Level/DEBUG
      :error org.apache.log4j.Level/ERROR
      :fatal org.apache.log4j.Level/FATAL
      :info org.apache.log4j.Level/INFO
      :off org.apache.log4j.Level/OFF
      :trace org.apache.log4j.Level/TRACE
      :warn org.apache.log4j.Level/WARN})

(defmethod log-level clojure.lang.Keyword
  [kw]
  (keyword-to-log-level kw))

(defn set-log4j-log-level
  "Sets the log level of the logger named by sym."
  [sym level]
  (.setLevel
   (org.apache.log4j.Logger/getLogger (str sym))
   (log-level level)))

(comment "The following examples do the same."
  (set-log4j-log-level 'ekahau.logutil :debug)
  (set-log4j-log-level 'ekahau.logutil org.apache.log4j.Level/DEBUG))