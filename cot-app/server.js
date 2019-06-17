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

app.get('/all-symbols', (request, response) => {
  connection.connect(function(err) {
    if (err) {
      console.log(err);
      throw err;
    }

    connection.query('SELECT * From symbols_data sd inner join symbols  s on (s.symbol_id =sd.symbol_id)', (err, res) => {
      const bySymbolId = {};

      res.forEach((row) => {
        bySymbolId[row.symbol_id] = [];
      });

      Object.keys(bySymbolId).forEach((symbolId) => {
        bySymbolId[symbolId] = [...bySymbolId[symbolId],  ...res.filter((row) => `${row.symbol_id}` === `${symbolId}`) ];
      });

      response.send(bySymbolId);
      connection.end();
    });
  });
});

app.listen(port);
