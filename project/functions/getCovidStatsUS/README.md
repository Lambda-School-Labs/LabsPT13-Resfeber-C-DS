# getCovidStatsUS AWS Lambda Function

### Objective

The `getCovidStatsUS` function calls the CDC data API and fetches daily state Covid stats and saves that data to an AWS RDS Postgres database

### Database Information

* DB Identifier: `labspt13-res`
* DB Username: `postgres`
* AWS Domain: `labspt13-res.c39zgmn8rrtf.us-east-2.rds.amazonaws.com`
* Password: `*******` (not listed)

### Triggers

The Lambda function is triggered in two ways: an 1) API REST request and a 2) recurring, daily, cron-like schedule

#### API Trigger

* URL: `https://9voqfm7104.execute-api.us-east-2.amazonaws.com/default/getCovidStatsUS`
* Request Headers:
    * `Content-Type`: `application/json`
    * `x-api-key`: `*******` (not listed)
* Optional Body:
    * e.g. `{"date": "2020-11-18"}`
    * no body fetches data for yesterday (today - 1 day)

#### API Request & Response

**Request**

```
curl --request POST \
  --url https://9voqfm7104.execute-api.us-east-2.amazonaws.com/default/getCovidStatsUS \
  --header 'Authorization: Basic Og==' \
  --header 'Content-Type: application/json' \
  --header 'x-api-key: lYLtISi42zaCz7X6VdjJ89d0JOw5yaRD4QYZGKfU'
```

```
curl --request POST \
  --url https://9voqfm7104.execute-api.us-east-2.amazonaws.com/default/getCovidStatsUS \
  --header 'Authorization: Basic Og==' \
  --header 'Content-Type: application/json' \
  --header 'x-api-key: <ADD API KEY HERE>' \
  --data '{"date": "2020-11-18"}'
```

**Response**

`"INFO: finished processing: 60 rows"`

#### Daily Trigger

See: https://us-east-2.console.aws.amazon.com/events/home?region=us-east-2#/eventbus/default/rules/getCovidStatsDaily

