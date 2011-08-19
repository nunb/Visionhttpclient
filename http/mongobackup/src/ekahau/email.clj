(ns ekahau.email
  (:require
    [clojure.contrib.java-utils])
  (:use
    [clojure.contrib.logging :only [info]])
  (:import
    [javax.mail Address Session Transport]
    [javax.mail.internet InternetAddress MimeMessage MimeMessage$RecipientType]))

(defn create-session
  [host port use-authentication]
  (when-not host
    (throw (IllegalArgumentException. "SMTP host not defined")))
  (Session/getInstance (clojure.contrib.java-utils/as-properties
                         (remove (comp nil? second)
                           [["mail.smtp.host" host]
                            ["mail.smtp.port" port]
                            ["mail.smtp.auth" (boolean use-authentication)]]))))

(defn- create-recipients
  [recipients]
  (into-array Address (map #(InternetAddress. %) recipients)))

(defn- create-mime-message
  [^Session session {:keys [from recipients subject text encoding] :or {encoding "UTF-8"}}]
  (doto (MimeMessage. session)
    (.setFrom (InternetAddress. from))
    (.setRecipients MimeMessage$RecipientType/TO ^"[Ljavax.mail.Address;" (create-recipients recipients))
    (.setSubject subject encoding)
    (.setText text encoding)
    (.setSentDate (java.util.Date.))
    (.saveChanges)))

(defn- send-email!*
  [^Session session ^Transport transport messages]
  (with-open [transport transport]
    (doseq [message messages]
      (info (str "Sending email: " message))
      (let [^MimeMessage mime-message (create-mime-message session message)]
        (.sendMessage transport mime-message (.getAllRecipients mime-message))))))

(defn- create-connected-transport
  ([^Session session]
    (doto (.getTransport session "smtp")
      (.connect)))
  ([^Session session ^String username ^String password]
    (doto (.getTransport session "smtp")
      (.connect username password))))

(defn send-email!
  ([session messages]
    (send-email!* session
      (create-connected-transport session)
      messages))
  ([session messages username password]
    (send-email!* session
      (create-connected-transport session username password)
      messages)))
