const express = require('express');
const bodyParser = require('body-parser');
const request = require('request');
const mysql = require('mysql');
const app = express();
const port = 3000;

const connection = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "arakis",
  database: "CoT"
});

app.listen(port);

connection.connect(function(err) {
  if (err) {
    throw err;
  }
});

connection.query('SELECT * From symbols_data sd inner join symbols  s on (s.symbol_id =sd.symbol_id)', (err, res) => {+
  const bySymbolId = {};

  res.forEach((row) => {
    bySymbolId[row.symbol_id] = { ...bySymbolId[row.symbol_id] }
  })

  const sorted = res.sort((a, b) => a.symbol_id !== b.symbol_id);

  const test = sorted.filter((a) => a.symbol_id === 640);
  console.log(test);

  // console.log(test.filter((val) => {
  //   return val.symbol_id === 619;
  // }).length);

})
