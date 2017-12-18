var spigot = require("stream-spigot")
var concat = require("concat-stream")
let moment = require('moment-timezone');
var bluepromise = require("bluebird");
let _ = require('lodash');
var agg = require("./aggregates");

var tData = [{
    "timestamp": 1507382468000,
    "engagement": 0,
    "media": {
        "caption": "Rest easy, there will come a good time when you'd have sand underneath you're feet and hot sun staring at your face till then, dream 🌅\n.\n.\n.\n.\n#srilanka #life #startuplife #startup #throwback #sky #beaches"
    }
}, {
    "timestamp": 1507514572000,
    "engagement": 18,
    "media": {
        "caption": "Sky 🌄\n.\n.\n.\n.\n.\n#sky #nature #travel #skyporn"
    }
}, {
    "timestamp": 1507805190000,
    "engagement": 102,
    "media": {
        "caption": "Le team presenting Alakazam 🔮, the Growth Sidekick for your business\n.\n.\n.\n #growth #startups #startuplife"
    }
}, {
    "timestamp": 1508116583000,
    "engagement": 73,
    "media": {
        "caption": "Haven't had a chance to go out of the dungeon (aka our office) for a while. Next few months are going to be even busier but hopefully more outside the dungeon 😁\n.\n.\n.\n#colombo #srilanka #startuplife #buildings #lka #startup #hustle"
    }
}, {
    "timestamp": 1508508606000,
    "engagement": 45,
    "media": {
        "caption": "I haven't noticed that I have been waking up at 6am and going to bed at 1 am for the past couple of months. Time really feels like flying when you are working on something that is going to do a groundbreaking change in the world.\n\n#startup #startuplife #coffeelife #lka"
    }
}, {
    "timestamp": 1509720055000,
    "engagement": 65,
    "media": {
        "caption": "Alakazam is at Infotel this year and we'll be here till Sunday, do come and drop by 🙌\n.\n.\n.\n#startuplife #startup #product #alakazam #madscience"
    }
}, {
    "timestamp": 1511667268000,
    "engagement": 43,
    "media": {
        "caption": "Some men just want to stare at the ocean and wait for code to compile.\n.\n.\n.\n. #madmen #startuplife #alakazam #startup"
    }
}, {
    "timestamp": 1511769598000,
    "engagement": 52,
    "media": {
        "caption": "Our beer sure has great branding. Love the Lion Lager branding as well. Now all we need is a stout can 🍺 .\n.\n.\n#beer #beerlovers #startuplife #beerlove #contemporaryart"
    }
}, {
    "timestamp": 1511876524000,
    "engagement": 36,
    "media": {
        "caption": "There are some days you get to have interesting conversations with interesting people and today was certainly one of them. Some interesting days are going to come about  #startup #startuplife #alakazam #srilanka #colombo"
    }
}];

function isFunction(functionToCheck) {
    var getType = {};
    return functionToCheck && getType.toString.call(functionToCheck) === '[object Function]';
}
function timeseries(opts, cb) {
    try {
        var localFunction;
        if (isFunction(opts.fcn)) {
            localFunction = agg['func'](opts.fcn)
        } else {
            localFunction = agg[opts.fcn];
        }

        spigot({ objectMode: true }, opts.data).pipe(localFunction(opts.timestampField, opts.interval, null, opts.tz || 'UTC')).pipe(concat(function (r) {
            if (!opts.interval) return cb(null, r);
            if (opts.fill == undefined) return cb(null, r);
            // console.log(moment(opts.start).tz(opts.tz || 'UTC').startOf(opts.interval), "start");
            // fit the sparse bins into strict bins
            let start = moment(opts.start).startOf(opts.interval);
            
            let end = opts.end;
            let bins = [];
            while (start.valueOf() <= end) {
                let b = {};
                b[opts.timestampField] = start.valueOf();
                let hit = _.find(r, b);
                if (hit) {
                    bins.push(hit);
                }
                else {
                    if (opts.indicateGenerated) {
                        b[opts.indicateGenerated] = true;
                    }
                    if (_.isNumber(opts.fill)) {
                        _.forIn(r[0], function (v, k) {
                            if (k == opts.timestampField) return;
                            if (typeof v == 'string') return;
                            b[k] = opts.fill;
                        });
                    }
                    else if (opts.fill == 'previous') {
                        let ref;
                        if (bins.length == 0) ref = r[0];
                        else ref = bins[bins.length - 1];
                        _.forIn(ref, function (v, k) {
                            if (k == opts.timestampField) return;
                            b[k] = (bins.length == 0) ? 0 : v;
                        });
                    }
                    else {
                        throw ('unsupported fill: ', opts.fill);
                    }
                    bins.push(b);
                }
                start = start.add(1, opts.interval);
            }
            cb(null, bins);
        }));
    } catch (err) {
        cb(err);
    }
};

/* 
  var options = {
      data: tData,
      timestampField: "timestamp",
      fcn: "sum" | "mean" | "median" | "mode" | "variance" | "percentile" 
      interval: "hour",
      tz: "America/Los_Angeles",
      fill: 0,
  };
*/
module.exports.rollup = function (options) {

    timeseriesFunction = bluepromise.promisify(timeseries);
    var dates = options.data.map(function (x) { return x[options.timestampField]; });

    var earliest = Math.min.apply(null, dates);
    var latest = Math.max.apply(null, dates);
    options.start = earliest;
    options.end = latest;
    return timeseriesFunction(options);
}