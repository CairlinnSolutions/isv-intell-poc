[![Deploy](https://www.herokucdn.com/deploy/button.svg)](https://heroku.com/deploy?template=https://github.com/kamipatel/aadataapp)

Prereq:
=============================
python3 -m venv env
source env/bin/activate

git clone the repo

pip install -r requirements.txt 

# Create Redis app
heroku addons:create heroku-redis:hobby-dev -a aadataapp
# To Flush Redis
heroku redis:cli -a aa --confirm aadataapp
$ flushall

Make sure you do this, if you get SSL cert error...
sudo /Applications/Python\ 3.6/Install\ Certificates.command

Testing:
=============================
Run local web app:
heroku local web

Run worker:
heroku local:run python worker.py

Post request
https://http://0.0.0.0:5000/api/aa
{"AppName":"CaseTimer", "packages":"0331U000000EHq2", "whichDate":"2019-07-25", "filelocation":""}

Updated code:
git add .
git commit -am "make it better"
git push heroku master


Tips:
=============================
https://heroku.com/deploy?template=

Flow wait formula
Now() - 58 * (1/24/60)

heroku local web


Additional info
================
'PackageIds': packages,
        'DataType': 'CustomObjectUsageLog',
        'StartTime': adaystart,
        'EndTime': adayend

PerformAARequest o = new PerformAARequest();

Datetime startdt = Datetime.newInstance(
    Date.today().addDays(-1),
    Time.newInstance(0, 0, 0, 0)
);

Datetime enddt = Datetime.newInstance(
    Date.today(),
    Time.newInstance(0, 0, 0, 0)
);

PerformAARequest.startaa('CaseTimer', '0331U000000EHq2', startdt,  enddt);
