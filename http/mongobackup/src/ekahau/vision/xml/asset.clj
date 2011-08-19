(ns ekahau.vision.xml.asset
  (:require
    [ekahau.vision.database :as database])
  (:use
    [clojure.contrib.str-utils :only [str-join]]
    [ekahau.vision.routes.helpers :only [get-attributes-from-entity str-id-path]]))

(defn format-property
  [value type]
  (when value
    (condp = type
        "database/calendar-day" (str-join "-" value)
        value)))

(defn asset-property-type-to-xml
  [type]
  (when type (database/db-type-to-name type)))

(defn- create-asset-property-element
  [id-path type value]
  [:property
   {:id (str-id-path id-path)
    :value (format-property value type)
    :type (asset-property-type-to-xml type)}])

(defn- create-asset-property-elements
  [asset get-value-type-by-property-id-path]
  (map
    (fn [[id-path value]]
     (let [type (get-value-type-by-property-id-path id-path) ]
       (create-asset-property-element id-path type value)))
    (:properties asset)))

(defn create-asset-element
  [asset get-value-type-by-property-id-path]
  [:asset (get-attributes-from-entity asset [:id :asset-type-id])
   (create-asset-property-elements asset get-value-type-by-property-id-path)])

(defn create-asset-elements
  [assets get-value-type-by-property-id-path]
  (map
    #(create-asset-element % get-value-type-by-property-id-path)
    assets))
