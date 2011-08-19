(ns ekahau.vision.todolist-service
  (:require
   [ekahau.vision.database :as database])
  (:use
   [clojure.contrib.trace :only [trace]]))

(defn get-new-todolist-id
  [db]
  (database/get-next-free-entity-id! db :todo-lists))

(defn- new-todo-list
  [stub]
  (apply struct-map database/todo-list (mapcat identity stub)))

(defn add-new-todo-list!
  [db stub]
  (database/put-entity! db :todo-lists (new-todo-list stub)))

(defn update-todo-list!
  [db id stub]
  (database/put-entity! db :todo-lists (assoc (new-todo-list stub) :id id)))

(defn delete-todo-list!
  [db id]
  (database/delete-entity! db :todo-lists id))

(defn get-todo-list-by-id
  [db id]
  (database/get-entity-by-id db :todo-lists id))

(defn get-user-todo-lists
  [db user-id]
  (database/search-entities db :todo-lists
    [["SOME-KEY=" [[:users] [:id]] user-id]]
    {:order-by [[[:due-date] 1] [[:id] 1]]}))

(defn- id-in
  [ids]
  ["IN" [:id] ids])

(defn items-by-asset-id
  [items]
  (into {} (map (juxt :asset-id identity) items)))

(defn add-asset-item-information
  [assets list-id items]
  (let [indexed-items (items-by-asset-id items)]
    (map
     (fn [asset]
       (let [item (get indexed-items (:id asset))]
         (assoc asset :to-do-list-information
                {:toDoListId list-id
                 :checkedOff (:checked-off? item)
                 :checkOffTime (:check-off-time item)})))
     assets)))

(defn update-item-of-to-do-list
  [db list-id asset-id f & params]
  (when-let [to-do-list (get-todo-list-by-id db list-id)]
    (database/put-entity!
     db :todo-lists
     (update-in to-do-list [:items]
                (fn [items]
                  (map
                   (fn [item]
                     (if (= asset-id (:asset-id item))
                       (apply f item params)
                       item))
                   items))))))

(defn check-to-do-list-item
  [db list-id asset-id json]
  (update-item-of-to-do-list
   db list-id asset-id
   (fn [item]
     (assoc item
       :checked-off? true
       :check-off-time (or (:checkOffTime json) (System/currentTimeMillis))))))

(defn uncheck-to-do-list-item
  [db list-id asset-id]
  (update-item-of-to-do-list
   db list-id asset-id
   (fn [item]
     (-> item
         (assoc :checked-off? false)
         (dissoc :check-off-time)))))
