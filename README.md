python3 -m venv env
source env/bin/activate

heroku addons:create heroku-redis:hobby-dev -a aabatch

Make sure you do this, if you get SSL cert error...
sudo /Applications/Python\ 3.6/Install\ Certificates.command

https://manage.auth0.com/dashboard/us/spring-hall-6290/apis/5d3b7371262229067e714eed/settings

curl --request POST \
  --url https://spring-hall-6290.auth0.com/oauth/token \
  --header 'content-type: application/json' \
  --data '{"client_id":"NNUH1qFrf5RJ4eF8V8lDUsGohSnX2Icp","client_secret":"PlkRqjuXgWHDQq6bVD11gQlEpyEhITY6WiJJutYwnVQjR47GMyjxpKR2ASzUQhbO","audience":"https://aabatch.herokuapp.com/","grant_type":"client_credentials"}'

curl --request GET \
--url http://0.0.0.0:5000/api/dojob \
  --header 'authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6Ik5VRTRORVl4TmpGR04wTTVNamMxTXpsRFF6WkRNMEkzUWpZMFJqUTBNVU5HUlRsQk16VXpSQSJ9.eyJpc3MiOiJodHRwczovL3NwcmluZy1oYWxsLTYyOTAuYXV0aDAuY29tLyIsInN1YiI6Ik5OVUgxcUZyZjVSSjRlRjhWOGxEVXNHb2hTblgySWNwQGNsaWVudHMiLCJhdWQiOiJodHRwczovL2FhYmF0Y2guaGVyb2t1YXBwLmNvbS8iLCJpYXQiOjE1NjQxODAzNTYsImV4cCI6MTU2NDI2Njc1NiwiYXpwIjoiTk5VSDFxRnJmNVJKNGVGOFY4bERVc0dvaFNuWDJJY3AiLCJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMifQ.fuYpBKfXBMtVQRnt4F8xUQJcO66u7Srb-zihqFmb4x7Pjf02pvyuX-Y2peICk7Nv8tM8P-qssMSrkRjKH6x0HjExJyCp6ZNElhY_O1IZmLbzAek3eZ8YweUV455dpa5cuP_JIULwmHkfrd6e4kgHPr61gETD70qMII61zSBehl6FAHB-Uu9l9GyHgIkusza7UOFd0sYwOU8HQVQ5YenoUeAxC2rs5BbDUfSHRcC6cqKTwse76ZlEp2u1UhtPKH3HEPJgKIlKAkeXEvWu0RO-chLlpsbVUOL_PHBhcy0zj953aDhO0oK8NPmwe3nTgZS8AGeKXGFp-SA58vsSreBtIw'

heroku local:run python worker.py
heroku local:run python manage.py queueprocessaa

heroku local web

git add .
git commit -am "make it better"
git push heroku master

# Flask Heroku Scheduler

This repo is a template for Flask Heroku deployment of scheduled jobs. We often run into the need to run "cronjob" like jobs on a server fr our machine learning pipelines. There are many ways to do this, but deploying to Heroku is the easiest. This code is supposed to be a template for how to do that quickly.

There are two files of interest for this app, the myapp.py, which contains a dummy web app that just returns simple text. This must be there because Heroku needs a web process to run.

The other file, manage.py, is more interesting. This is where you can define commands to be run either on-demand or by the scheduler. This file is written using flask-script, a lightweight management plugin for Flask. We have defined only one command in there, hello. You can run it locally by typing:

```
python manage.py hello
```

If you have the Heroku CLI installed, you can type this instead:

```
heroku local:run python manage.py hello
```

This same job can be run on-demand on a deployed Heroku app by typing:

```
heroku run python manage.py hello
```

The scheduler uses this same interface to run. To use it, go to the web portal for your Heroku app and configure the [Heroku Scheduler plugin](https://devcenter.heroku.com/articles/scheduler) for your app. Then just setup a job like below.

![Scheduler](scheduler.png)

After running, you should see these lines in your Heroku logs. You can see these logs by simply typing:

```
heroku logs
```

![Logs](logs.png)
