const express = require('express');
const mysql = require('mysql');
const app = express();
const port = 3000;
const moment = require('moment');

const mapping = {
  'name': ' Name',
  'open_interest': 'Open Interest Gesamt',
  'comm_long_oi': 'Commercial Long OI %',
  'comm_short_oi': 'Commercial Short OI %',
  'comm_long': 'Commercial Long',
  'comm_short': 'Commercial Short',
  'comm_netto': 'Commercial Netto',
  'large_netto': 'Large Netto',
  'large_long': 'Large Long',
  'large_long_oi': 'Large Long OI %',
  'large_short': 'Large Short',
  'large_short_oi': 'Large Short OI %',
  'small_netto': 'Small Netto',
  'small_long': 'Small Long',
  'small_long_oi': 'Small Long OI %',
  'small_short': 'Small Short',
  'small_short_oi': 'Small Short OI %'
};
																							

app.get('/all-symbols', (request, response) => {
  const connection = mysql.createConnection({
    host: "localhost",
    port: "3309",
    user: "cot",
    password: "cot",
    database: "cot"
  });
  connection.connect(function(err) {
    if (err) {
      console.log(err);
      throw err;
    }

    connection.query('SELECT * From symbols_data sd inner join symbols s on (s.symbol_id =sd.symbol_id) ORDER BY sd.symbol_id asc, week desc', (err, res) => {
      const bySymbol = res.reduce((acc, row, i, arr) => {
        if (!acc[row.symbol_id]){
          acc[row.symbol_id] = [];
        }
        acc[row.symbol_id].push(row);
        return acc;
      }, {});
      const out = Object.values(bySymbol).map(symbol => {
        const data = {};
        symbol.forEach((row, i) => {
          const weekFormat = `(Woche ${("0"+(i+1)).substr(-2)} - ${moment(row.week).format(' - w/YY')})`;
          Object.keys(row).forEach(key => {
            if (key === 'name') {
              data[mapping[key]] = row[key];
            } else if (mapping[key]){
              data[`${mapping[key]} ${weekFormat}`] = row[key];
            }
          });
        });
        const ordered = {};
        Object.keys(data).sort().forEach(function(key) {
          ordered[key] = data[key];
        });
        return ordered;
      });

      response.send(out);
      connection.end();
    });
  });
});

app.listen(port);

process.on('uncaughtException', function (err) {
  console.log('uncaughtException', err);
  if (err.stack) {
    console.dir(err.stack);
  }
  console.trace("Error occured at:");
});