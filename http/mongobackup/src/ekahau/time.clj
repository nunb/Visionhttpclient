(ns ekahau.time
  (:import
    [java.text SimpleDateFormat]
    [java.util Calendar TimeZone])
  (:use
    [clj-time.core :only (now interval plus minus start end)]
    [clj-time.coerce :only (to-long)])
  (:import
    [org.joda.time DateTime Interval]))

(defn- time-today
  [hour-of-day minute-of-hour second-of-minute millis-of-second]
  (.toDateTime (.withTime (.toLocalDateTime ^DateTime (now)) hour-of-day minute-of-hour second-of-minute millis-of-second)))

(defn- interval-of-duration-from-start
  [start duration]
  (interval start (plus start duration)))

(defn interval-to-millis
  [^Interval an-interval]
  [(.getStartMillis an-interval) (.getEndMillis an-interval)])

(defn interval-duration-until-now
  [duration]
  (let [end (now)]
    (interval (minus end duration) end)))

(defn- interval-to-long-vec
  [interval]
  [(to-long (start interval)) (to-long (end interval))])

(defonce NUMBER_OF_MILLIS_IN_A_DAY (* 1000 60 60 24))

(defn number-of-days-since-epoch
  [^Long millis]
  (long (/ millis NUMBER_OF_MILLIS_IN_A_DAY)))
