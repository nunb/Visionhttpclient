(ns ekahau.vision.location-history-report.base-report)

(defmulti prepare-report :type)

(defmethod prepare-report :default
  [report] report)