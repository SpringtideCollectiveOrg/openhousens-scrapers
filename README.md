# Pupa Scrapers for Nova Scotia in Ruby

The [bulk of Pupa scrapers](http://github.com/opencivicdata/scrapers-ca) for Canada are written in Python.

## Getting Started

Install Git, Ruby 2.x and MongoDB. We recommend [rbenv](https://github.com/sstephenson/rbenv) to manage your Rubies.

Install the Bundler gem:

    gem install bundler

Get the code:

    git clone https://github.com/opennorth/openhousens-scrapers.git
    cd openhousens-scrapers

Install gem dependencies:

    bundle

Run a scraper with, for example:

    ruby ca_ns/scraper.rb

## API

    foreman start

* `GET /memberships?in_network_of=ocd-organization/country:ca/province:ns/legislature`
* `GET /organizations?in_network_of=ocd-organization/country:ca/province:ns/legislature`
* `GET /people?member_of=ocd-organization/country:ca/province:ns/legislature`
* `GET /posts?organization_id=ocd-organization/country:ca/province:ns/legislature`

## Deployment

    heroku addons:add flydata
    heroku addons:add mongolab
    heroku addons:add rediscloud
    heroku addons:add scheduler

Add configuration variables (replace `REPLACE`):

    heroku config:add AWS_BUCKET=REPLACE
    heroku config:add AWS_ACCESS_KEY_ID=REPLACE
    heroku config:add AWS_SECRET_ACCESS_KEY=REPLACE

Schedule jobs to run daily, for example:

    ruby ca_ns/scraper.rb --pipelined -q -t people
    ruby ca_ns/scraper.rb --pipelined -q -a scrape -a import -a akoma_ntoso -- down-to `date +%Y-%m-%d` no-clobber 1

Copyright (c) 2013 Open North Inc., released under the MIT license
