const XLSX = require('xlsx');
const request = require('request');
const mysql = require('mysql');
const moment = require('moment');
const { Observable, from, merge, combineLatest, of } = require('rxjs');
const { filter, reduce, concat, concatAll, mergeMap, map, switchAll, mergeAll, switchMap, mapTo, share, tap, zip, combineAll } = require('rxjs/operators');
const cheerio = require('cheerio');

let symbols = {};
const excelFiles = [
  {
    name: 'CoT_Daten_2019.xlsx',
    url: 'https://s1ee9bc4c878f2f5f.jimcontent.com/download/version/1556307196/module/7553454581/name/CoT%20Daten%202019.xlsx'
  },
  // {
  //   name: 'CoT_Daten_2010-2017.xlsx',
  //   url: 'https://s1ee9bc4c878f2f5f.jimcontent.com/download/version/1529697217/module/7339046781/name/CoT%20Daten%202010%20-%202017.xlsx'
  // },
  // {
  //   name: 'CoT_Daten_1990-1999.xlsx',
  //   url: 'https://s1ee9bc4c878f2f5f.jimcontent.com/download/version/1529697250/module/7339047381/name/CoT%20Daten%201990%20-%201999.xlsx'
  // },
  // {
  //   name: 'CoT_Daten_2018.xlsx',
  //   url: 'https://s1ee9bc4c878f2f5f.jimcontent.com/download/version/1550004467/module/7553454681/name/CoT%20Daten%202018%20.xlsx'
  // },
  // {
  //   name: 'CoT_Daten_2000-2009.xlsx',
  //   url: 'https://s1ee9bc4c878f2f5f.jimcontent.com/download/version/1529697228/module/7339046881/name/CoT%20Daten%202000%20-%202009.xlsx'
  // },
  // {
  //   name: 'CoT_Daten_1986-1989.xlsx',
  //   url: 'https://s1ee9bc4c878f2f5f.jimcontent.com/download/version/1529697265/module/7339047581/name/CoT%20Daten%201986%20-%201989.xlsx'
  // }
];

const mysqlConnect = () => mysql.createConnection({
  host: "localhost",
  // port: "3309",
  user: "root",
  password: "arakis",
  database: "CoT"
});

const options = {
  method: 'GET',
  encoding: null
};

const requestData = url => {
  return Observable.create(observer => {
    request(url, (err, res, body) => {
      if (err) {
        observer.error(err);
      } else if (res.code < 200 || res.code > 399) {
        observer.error(res);
      } else {
        observer.next(body);
      }
    })
  })
};

const getRows = $ => {
  const out = [];
  const rowKeys = [];
  $('tr.head').find('td').each((idx, val) => {
    rowKeys.push($(val).text());
  });

  $('tr.d').each((i, node) => {
    const row = {};
    $(node).find('td').each((idx, val) => {
      row[rowKeys[idx]] = $(val).text();
    });
    row.date = moment($('p').text().replace('Stand: ', ''), 'DD-MM-YYYY').toDate();
    out.push(row);
  });

  return out;
  return from(out);
};

const getUrls = $ => {
  return from($('div.link a').map((i, link) => {
    if (i <= 25) {
      return `https://cnt1.suricate-trading.de/cotde/${link.attribs.href}`;
    }
  }));
};

const query = (connection) => (query) => {
  return Observable.create(observer => {
    connection.query(query, (err, result) => {
      if (err) {
        observer.error(err);
      } else {
        // observer.next(result);
        result.map(dataPack => observer.next(dataPack))
      }
    });
  });
};

const getSymbols = connection => query(connection)(`SELECT * FROM symbols`)
  .pipe(
    map(({ name, symbol_id }) => {
      return of({ name, symbol_id });
    }),
    mergeAll()
);

const connect = () => {
  return Observable.create((observer) => {
    const connection = mysqlConnect();
    connection.connect(err => {
      if (err)
        observer.error(err);
      else {
        observer.next(connection);
      }
    })
  })
};

const requestDataStream = requestData('https://cnt1.suricate-trading.de/cotde/cot-history.html').pipe(
  map(body => cheerio.load(body)),
  map(html => getUrls(html)),
  switchAll(),
  map(url => {
    return requestData(url);
  }),
  mergeAll(),
  map(tableBody => cheerio.load(tableBody)),
  map($ => getRows($)),
  share()
)
//   .subscribe((data) => {
//   console.log(data);
// });

const symbolsTableStream = connect().pipe(
  map(connection => getSymbols(connection)),
  // switchAll(),
  concatAll(),
  tap((symbols) => {
    console.log(symbols);
  })
).subscribe(symbol => symbol);

const getFilteredRowDataKeys = (dataRow) => {
  const symbolsNames = symbols.map((sym) => {
    return sym.name;
  });

  return Object.keys(dataRow).filter((key) => {
    return !symbolsNames.includes(dataRow[key]);
  });
};

const getWeeksKeys = (weeks, dataRow) => {
  const filteredRowDataKeys = getFilteredRowDataKeys(dataRow);
  const totalWeeks = Object.keys(weeks).length;
  const allWeeksKeys = {};
  const getLengthOfRowByDate = () => filteredRowDataKeys.length / totalWeeks;
  let weekKeys = [];
  let weekIndex = 0;
  let lengthOfQueryRow = getLengthOfRowByDate();

  filteredRowDataKeys.forEach((key, index) => {
    if (index <= lengthOfQueryRow) {
      weekKeys.push(key);

      if (index === lengthOfQueryRow - 1) {
        allWeeksKeys[weeks[Object.keys(weeks)[weekIndex]]] = weekKeys;
        weekKeys = [];
        weekIndex++;
        lengthOfQueryRow += getLengthOfRowByDate();
      }
    }
  });

  return allWeeksKeys;
};

