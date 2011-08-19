(ns ekahau.vision.model.event-rule
  (:use ekahau.entity))

(comment "Example Rule"
  {:name "Ping"
   :description "Just a button ping"
   :engine-id "1"
   :trigger {:type "button"
             :buttons ["button1" "button2" "menu-button"]
             :subject-specification {:type "all"}
             :area-specification {:type "all"}}
   :action {:type "composite",
            :components
            [{:type "notification",
              :target {:type "all-users"},
              :close-button-label "",
              :title ""}]}})

(def valid-buttons #{"button1" "button2" "menu"})

(defentity action {:base-type true})

;; Tag message ;;
(defentity tag-message-target {:base-type true})

(defentity originator-tag-message-target {:extends tag-message-target :as :originator-tag})

(defentity all-display-tags-tag-message-target {:extends tag-message-target :as :all-display-tags}
  :exclude-originator-tag {:type :boolean})

(defentity asset-groups-tag-message-target {:extends tag-message-target :as :asset-groups}
  :ids {:type :list :element-type :id}
  :exclude-originator-tag {:type :boolean})

(defentity assets-tag-message-target {:extends tag-message-target :as :assets-tag}
  :ids {:type :list :element-type :id}
  :exclude-originator-tag {:type :boolean})

(def message-target-entities (entity-map [originator-tag-message-target
                                          all-display-tags-tag-message-target
                                          asset-groups-tag-message-target
                                          assets-tag-message-target]))

(defentity tag-message-action {:extends action :as :tag-message}
  :message {:type :string}
  :instant {:type :boolean}
  :target  {:type :entity :entity-type tag-message-target})


;; Emergin message ;;
(defentity emergin-message-action {:extends action :as :emergin-message}
  :message {:type :string}
  :source-id {:type :string}
  :sensitivity {:type :string})

;; User target ;;
(defentity user-target {:base-type true})

(defentity all-users-user-target {:extends user-target :as :all-users})

(defentity selected-users-user-target {:extends user-target :as :selected-users}
  :recipients {:type :element-list :value-type :string})

(def user-target-entities (entity-map [all-users-user-target
                                       selected-users-user-target]))

;; Notification Action ;;
(defentity notification-action {:extends action :as :notification}
  :title {:type :string}
  :close-button-label {:type :string}
  :target {:type :entity :entity-type user-target})

;; Alert Action ;;
(defentity alert-action {:extends action :as :alert}
  :title {:type :string}
  :instructions {:type :string}
  :accept-button-label {:type :string}
  :decline-button-label {:type :string}
  :close-button-label {:type :string}
  :target {:type :entity :entity-type user-target})

;; Email Action ;;
(defentity email-action {:extends action :as :email}
  :message-subject {:type :string}
  :message-text {:type :string}
  :do-not-format {:type :boolean}
  :recipients {:type :element-list :value-type :string})

;; Composite Action ;;
(defentity composite-action {:extends action :as :composite :composite-of action})

;; httpget action ;;
(defentity httpget-action {:extends action :as :httpget}
 :url {:type :string})



