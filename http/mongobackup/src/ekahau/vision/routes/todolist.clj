(ns ekahau.vision.routes.todolist
  (:require
   [clojure.java.io :as io]
   [clojure.contrib.json :as json]
   [ekahau.vision.todolist-service :as tds]
   [ekahau.vision.json.asset :as json-asset])
  (:use
    compojure.core
    ekahau.vision.routes.helpers
    [clojure.contrib.logging :only [error]]))

(defn- get-todolist-response
  [db request]
  (with-useful-bindings request
    {:id todolist-id}
    (try
      (to-json (tds/get-todo-list-by-id db todolist-id))
      (catch Exception e
        (error "Error fetching todolist" e)
        (create-bad-request-response request (.getMessage e) to-json)))))

(defn- get-todolists-response
  [db request]
  (with-useful-bindings request
    {:user user}
    (try
      (to-json (tds/get-user-todo-lists db (:uid user)))
      (catch Exception e
        (error "Error fetching todolists" e)
        (create-bad-request-response request (.getMessage e) to-json)))))

(defn- ensure-users
  "Makes sure that todolist contains at least one user by setting the creating
  user as a sole member of users list if it would be otherwise empty."
  [todolist user]
  (if (empty? (:users todolist))
    (assoc todolist
      :users [{:id (:uid user)}])
    todolist))

(defn- new-todolist-response
  [db request]
  (with-useful-bindings request
    {:json-body todolist-stub
     :user user}
    (try
      (let [id (tds/get-new-todolist-id db)
            todolist-stub (-> (assoc todolist-stub
                                :id id)
                              (ensure-users user))]
        (tds/add-new-todo-list! db todolist-stub)
        (to-json todolist-stub))
      (catch Exception e
        (error "Error adding new todo list" e)
        (create-bad-request-response request (.getMessage e) to-json)))))

(defn update-todolist-response
  [db request]
  (with-useful-bindings request
    {:json-body todolist-stub
     :id todolist-id}
    (try
      (tds/update-todo-list! db todolist-id todolist-stub)
      (to-json (tds/get-todo-list-by-id db todolist-id))
      (catch Exception e
        (error "Error updating todo list" e)
        (create-bad-request-response request (.getMessage e) to-json)))))

(defn delete-todolist-response
  [db request]
  (with-useful-bindings request
    {:id todolist-id}
    (try
      (tds/delete-todo-list! db todolist-id)
      (catch Exception e
        (error "Error deleting todo list" e)
        (create-bad-request-response request (.getMessage e) to-json)))))

(defn create-todolist-routes
  [db]
  (-> (routes
       (GET "/todoList/:id" {:as request}
            (get-todolist-response db request))
       (GET "/todoList" {:as request}
            (get-todolists-response db request))
       (POST "/todoList" {:as request}
             (new-todolist-response db request))
       (PUT "/todoList/:id" {:as request}
            (update-todolist-response db request))
       (PUT "/todoList/:id/:asset/checked.json" {{:keys [id asset]} :params
                                                 body :body}
            (tds/check-to-do-list-item db id asset (json/read-json (io/reader body)))
            (to-json "ok"))
       (PUT "/todoList/:id/:asset/unchecked.json" {{:keys [id asset]} :params}
            (tds/uncheck-to-do-list-item db id asset)
            (to-json "ok"))
       (DELETE "/todoList/:id" {:as request}
               (delete-todolist-response db request)))
      (with-json-content-type)))
