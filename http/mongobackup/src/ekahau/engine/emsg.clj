(ns ekahau.engine.emsg
  (:import
    [java.util Map$Entry]
    [com.ekahau.common.sdk EMsg EMsgProperty EResponse]))

(defstruct emsg :type :properties)

(defn- make-emsg
  [m]
  (let [result (EMsg.)]
    (doseq [[k v] m]
      (if (coll? v)
        (.add result (name k) ^"[Ljava.lang.String;" (into-array String (map str v)))
        (.add result (name k) (str v))))
    result))

(defn make-emsg-array
  [ms]
  (into-array EMsg (map #(make-emsg %) ms)))

(defn- unpack-value
  [value]
  (if (.isArray (class value))
    (seq value)
    value))

(defn- unpack-emsg-properties
  [^EMsg msg]
  (let [properties (iterator-seq (. msg iterator))]
    (reduce #(assoc %1 (keyword (.getKey ^EMsgProperty %2))
               (unpack-value (.getValue ^EMsgProperty %2)))
      {}
      properties)))

(defn- unpack-emsg
  [^EMsg msg]
  (struct emsg (. msg getMsgType) (unpack-emsg-properties msg)))

(defn unpack-eresponse
  [^EResponse eresponse]
  (map unpack-emsg (seq (. eresponse get))))
