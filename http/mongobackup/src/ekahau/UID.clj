(ns ekahau.UID
  (:require
    [clojure.contrib.json])
  (:import
    org.bson.types.ObjectId
    [java.util Date]
    [java.io Serializable PrintWriter]))

(defprotocol A-Vision-UID
  
  "Abstraction to generate Ids for Vision Database.
   The intent is the provide an abstraction for something along the lines of - 
   http://en.wikipedia.org/wiki/Universally_unique_identifier
   But it doesn't necessarily have to a UUID (as defined by the spec).

   Besides,
     1. Being A-Vision-UID
   A uid for Vision database also
     2. Need to implement proper equals and hashcode
     3. Needs to implement Comparable
        Sorting should be based on time (precision=atleast second, then arbitrary) 
     4. Needs to implement Serializable
        The toString (str) method needs to return the (de)serializable form."
  
  (get-time [this]))

; A UID is just .. you guessed it .. String!
; Strings automatically satisfy 2., 3., 4.
; Plus, and this is where they're really convinient -
; they don't need to go through all those serialization & deserialization layers
; all dbs have a String datatype (aka varchar)

; One slight disadvantage with having uid as String and not ObjectId is:
; (do (def o1 (ObjectId.)) (def o2 (ObjectId.)))
; It is guaranteed that (compare o1 o2) < 0
; But is possible that (compare (str o1) (str o2)) > 0
; eg "4cd381db7fd0952bff466f8c" "4cd381db7fd0952b00476f8c"
; This may happen only when (= (.getTime o1) (.getTime o2))
; But its not a cause of concern.

(extend String
  A-Vision-UID
  {:get-time (fn [^String this] (.getTime (ObjectId. this)))})

(defn is-valid?
  [uid-look-alike]
  (string? uid-look-alike))

(defn gen
  []
  (str (ObjectId.)))

; Not really theoretically minimum, but practically good enough.
; any of _time, _machine, _inc can be -ve also.
; As such there's a bug in ObjectId in case of:
; user=> (def o1 (ObjectId. 0 0 0))
; user=> (def o2 (ObjectId. 0 Integer/MIN_VALUE Integer/MIN_VALUE))
; user=> (compare o2 o1)
; -2147483648
; user=> (compare o1 o2)
; -2147483648
; It's got to do with Integer overflow.
(def MIN-UID (str (ObjectId. 0 0 0)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;; HELPERS ;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn parse-id
  [uid-look-alike]
  (when uid-look-alike
    (if (is-valid? uid-look-alike)
      uid-look-alike
      (throw (IllegalArgumentException. (str uid-look-alike " can't be parsed as uid"))))))

;(extend ekahau.UID.A_Vision_UID clojure.contrib.json/Write-JSON
;  {:write-json (fn [x ^PrintWriter out]
;                 (.print out (str x)))})
;
;(deftype MongoDBObjectID
;  [^ObjectId oid]
;  
;  A-Vision-UID
;  (get-time [this] (.getTime oid))
;  
;  Object
;  (equals [this o] (if (= MongoDBObjectID (class o))
;                     (.equals oid (.oid o))
;                     false))
;  (hashCode [this] (.hashCode oid))
;  (toString [this] (.toString oid))
;  
;  Comparable
;  (compareTo [this o] (.compareTo oid (.oid o)))
;  
;  Serializable)
;
;(defn gen
;  ([]
;    (MongoDBObjectID. (ObjectId.)))
;  ([o]
;   (MongoDBObjectID. (ObjectId. o))))
;
;(def parse-id gen)
;
;(defn is-valid?
;  [uid-look-alike]
;  (if (instance? MongoDBObjectID uid-look-alike)
;    true
;    (try
;      (gen uid-look-alike)
;      true
;      (catch Exception _ false))))
;
