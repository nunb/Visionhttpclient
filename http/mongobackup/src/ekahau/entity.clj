(ns ekahau.entity
  (:use
    [clojure.contrib.condition :only [raise]]
    [clojure.contrib.str-utils :only [re-split str-join]]
    [clojure.contrib.trace :only [trace]]
    [ekahau.string :only [parse-id]]
    [ekahau.util :only [camel-to-clojure clojure-keyword-to-camel clojure-to-camel]]))

(defn- update-if
  [x pred f & args]
  (if (pred x)
    (apply f x args)
    x))

(defn- get-value-attribute-keys
  [params]
  (->> params
    (partition 2)
    (remove (fn [[_ {type :type}]] (#{:entity} type)))
    (map first)
    (vec)))

(defn- get-entity-attribute-keys
  [params]
  (->> params
    (partition 2)
    (filter (comp #{:entity} :type second))
    (map first)
    (vec)))

(declare entity->xml
	 xml->entity)

(defn- get-xml-symbols
  [name]
  {:pre [(not (nil? name))]}
  {:xml->entity (symbol (str "xml->" name))
   :entity->xml (symbol (str name "->xml"))})

(defn- basetype?
  [metadata]
  (:base-type metadata))

(defn- subtype?
  [metadata]
  (:extends metadata))

(defmacro defmulti-basetype-xml
  [name]
  (let [{:keys [xml->entity entity->xml]} (get-xml-symbols (clojure.core/name name))]
    `(do
       (defmulti ~xml->entity (fn [xml#] (-> xml# :attrs :type keyword)))
       (defmulti ~entity->xml (fn [entity#] (-> entity# :type keyword))))))

(defmacro defmethod-extended-type-xml
  [metadata entity]
  (let [{:keys [xml->entity entity->xml]} (get-xml-symbols (-> metadata :extends))
        clojure-type (-> metadata :as)
        xml-type ( clojure-keyword-to-camel clojure-type)]
    `(do
       (defmethod ~xml->entity ~xml-type
         [xml#]
         (xml->entity ~entity xml#))
       (defmethod ~entity->xml ~clojure-type
         [entity#]
         (entity->xml ~entity entity#)))))

(defmacro defentity-multimethods
  [name metadata entity]
  (cond
    (basetype? metadata)
    `(defmulti-basetype-xml ~name)
    
    (subtype? metadata)
    `(defmethod-extended-type-xml ~metadata ~entity)))

(defn assoc-xml-functions
  {:private true}
  [name result-map]
  (if-let [base-class (-> result-map :metadata :extends)]
    (let [{:keys [xml->entity entity->xml]} (get-xml-symbols (clojure.core/name base-class))]
      (assoc result-map
        :xml->entity (resolve xml->entity)
        :entity->xml (resolve entity->xml)))
    (let [{:keys [xml->entity entity->xml]} (get-xml-symbols (clojure.core/name name))]
      (assoc result-map
        :xml->entity (resolve xml->entity)
        :entity->xml (resolve entity->xml)))))

(defmacro defentity*
  [name result-map]
  (let [result-map (assoc-xml-functions name result-map)]
    `(def ~name ~result-map)))

(defmacro defentity
  [name & params]
  (let [metadata (when (map? (first params)) (first params))
        attr-params (if metadata (drop 1 params) params)
        ks (get-value-attribute-keys attr-params)
        key-metadata (select-keys (apply hash-map attr-params) ks)
        entity-ks (get-entity-attribute-keys attr-params)
        entity-key-metadata (select-keys (apply hash-map attr-params) entity-ks)
        result-map (-> {:name (keyword name)
                        :metadata metadata
                        :keys ks
                        :key-metadata key-metadata
                        :entity-keys entity-ks
                        :entity-key-metadata entity-key-metadata})]
    `(do
       (defentity-multimethods ~name ~metadata ~result-map)
       (defentity* ~name ~result-map))))

(defn entity-map
  [entities]
  (reduce
    (fn [m entity]
      (assoc m (-> entity :metadata :as) entity))
    {} entities))

(defn- get-entity-attribute-metadata
  [entity attribute]
  (or (get-in entity [:key-metadata attribute]) [:unknown attribute]))

(defn create-entity
  [entity & params]
  (apply hash-map params))

;; Converting strings to attribute values ;;

(defmulti str->attribute-value {:private true} (fn [type s] (:type type)))

(defmethod str->attribute-value :string
  [type s]
  s)

(defmethod str->attribute-value :integer
  [type s]
  (Integer/parseInt s))

(defmethod str->attribute-value :boolean
  [type s]
  (let [result (get {"true" true "false" false} s)]
    (when (nil? result)
      (raise :type :invalid-value :value s))
    result))

(defmethod str->attribute-value :list
  [type s]
  (let [parts (re-split #"," s)]
    (condp = (:element-type type)
      :id (map parse-id parts)
      nil)))

;; Converting attribute values to strings ;;

(defmulti attribute-value->str {:private true} (fn [type value] (:type type)))

(defmethod attribute-value->str :string
  [type value]
  value)

(defmethod attribute-value->str :integer
  [type value]
  (str value))

(defmethod attribute-value->str :boolean
  [type value]
  (if value "true" "false"))

(defmethod attribute-value->str :list
  [type value]
  (condp = (:element-type type)
    :id (str-join "," (sort value))
    nil))

;; Converting entities to and from XML ;;

(defn- entity-attribute->str
  [entity attribute value]
  (attribute-value->str (get-entity-attribute-metadata entity attribute) (get value attribute)))

(defn- xml-attrs-from-entity
  [entity value]
  (->> (:keys entity)
    (remove #(-> entity :key-metadata % :type (= :element-list)))
    (map (fn [k] [(clojure-keyword-to-camel k) (entity-attribute->str entity k value)]))
    (remove (comp nil? #(get % 1)))
    (into (if-let [parent (-> entity :metadata :extends)]
            (xml-attrs-from-entity parent value)
            {}))))

(defn- get-entity-tag-name
  [entity]
  (if-let [parent (-> entity :metadata :extends)]
    (get-entity-tag-name parent)
    (clojure-keyword-to-camel (:name entity))))

(defn- get-entity-type-attribute
  [entity]
  (when-let [n (-> entity :metadata :as)]
    {:type (clojure-to-camel (name n))}))

(defn- nil-or-vec
  [coll]
  (when-not (empty? coll)
    (vec coll)))

(defn- get-xml-content-from-entity-attributes
  [value entity-map]
  (->> entity-map
    (map
      (fn [[attribute entities]]
        (when-let [attribute-value (get value attribute)]
          (assoc (entity->xml (get entities (keyword (:type attribute-value))) attribute-value)
            :tag attribute))))
    (remove nil?)
    (nil-or-vec)))

(defn- get-xml-content-from-entity-attribute-using-entity
  [entity value]
  (if-let [component-type (-> entity :metadata :composite-of)]
    (let [component->xml (:entity->xml component-type)]
      (->> (:components value)
        (map component->xml)
        (nil-or-vec)))
    (-> []
      (into
        (->> (:entity-keys entity)
          (map
            (fn [k]
              (let [md (-> entity :entity-key-metadata k)]
                (assoc ((-> md :entity-type :entity->xml) (get value k)) :tag (clojure-keyword-to-camel k)))))
          (remove nil?)))
      (into
        (->> (:keys entity)
          (filter #(-> entity :key-metadata % :type (= :element-list)))
          (map (fn [k]
                 {:tag (clojure-keyword-to-camel k)
                  :attrs nil
                  :content (->> (get value k)
                             (sort)
                             (map (fn [v]
                                    {:tag :value
                                     :attrs nil
                                     :content [v]})))}))))
      (nil-or-vec))))

(defn entity->xml
  ([entity value]
    (entity->xml entity value nil))
  ([entity value entity-map]
    {:tag (get-entity-tag-name entity)
     :attrs (merge (xml-attrs-from-entity entity value)
              (get-entity-type-attribute entity))
     :content (if (and entity-map (< 0 (count entity-map)))
                (get-xml-content-from-entity-attributes value entity-map)
                (get-xml-content-from-entity-attribute-using-entity entity value))}))

(defn- keyword-from-camel-case-to-hyphenated
  [kw]
  (-> kw name camel-to-clojure keyword))

(defn- entity-attributes-from-xml
  [entity xml]
  (let [attrs (:attrs xml)]
    (reduce
      (fn [value k]
        (assoc value k
          (let [metadata (get-entity-attribute-metadata entity k)]
            (condp = (:type metadata)
              :element-list (let [values (first (filter (comp (partial = k) :tag) (:content xml)))]
                              (seq (set (map (comp first :content) (:content values)))))
              (str->attribute-value metadata (get attrs (clojure-keyword-to-camel k)))))))
      {} (:keys entity))))

(defn- get-type-key
  [xml]
  (when-let [type (-> xml :attrs :type)]
    (-> type camel-to-clojure keyword)))

(defn xml-tag-to-kw
  [xml]
  (-> xml :tag keyword-from-camel-case-to-hyphenated))

(defn- xml->entity-attribute
  [entity-map xml]
  (let [k (xml-tag-to-kw xml)]
    (when-let [component-entity (get entity-map (get-type-key xml))]
      {k (xml->entity component-entity xml)})))

(defn- entity-component-attributes-from-xml
  [entity xml entity-map]
  (if (and entity-map (< 0 (count entity-map)))
    (->> (:content xml)
      (map (partial xml->entity-attribute entity-map))
      (remove nil?)
      (apply merge))
    (let [entity-key-metadata (-> entity :entity-key-metadata)]
      (->> (:content xml)
        (map (fn [xml]
               (let [tag (xml-tag-to-kw xml)]
                 (when-let [metadata (get entity-key-metadata tag)]
                   {tag ((-> metadata :entity-type :xml->entity) xml)}))))
        (remove nil?)
        (apply merge)))))

(defn- xml->composite-entity
  [entity xml]
  (println "the complete xml is " xml) 
  (println "the entity-metadata-composite-of is :" (-> entity :metadata :composite-of))
  (let [composite-type (-> entity :metadata :composite-of)
        component-xml->entity (:xml->entity composite-type)]
    (println "the composite-type is " composite-type) 
    (println "the component-xml->entity is " component-xml->entity) 
    (println "the content in the xml is " (:content xml)) 
    {:type "composite"
     :components (let [something (dorun (map component-xml->entity (:content xml)))] 
                   (println "something value is " something) 
                   (vec something))}))

;(->> (:content xml)
;                   (map component-xml->entity)
;                   (vec))

(defn xml->entity
  ([entity xml]
    (println "the entity is : " entity)
    (xml->entity entity xml {}))
  ([entity xml entity-map]
    (if (-> entity :metadata :composite-of)
      (xml->composite-entity entity xml)
      (let [attrs (merge (entity-attributes-from-xml entity xml)
                    (entity-component-attributes-from-xml entity xml entity-map))]
        (if-let [type (-> entity :metadata :as)]
          (assoc attrs :type (name type))
          attrs)))))
