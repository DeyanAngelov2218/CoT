const XLSX = require('xlsx');
const request = require('request');
const mysql = require('mysql');
const moment = require('moment');
const { Observable, Subject, from, merge, combineLatest, of } = require('rxjs');
const { exhaustMap, groupBy, take, concatMap, toArray, scan, filter, reduce, concat, flatMap, concatAll, mergeMap, map,
  switchAll, mergeAll, switchMap, mapTo, share, tap, zip, combineAll, bufferCount } = require('rxjs/operators');
const cheerio = require('cheerio');

class Repository {
  constructor() {
    const that = this;
    this.connection = null;
    this.symbols = [];
    this.getSymbols$().pipe(
      scan((acc, symbol) => [symbol, ...acc], [])
    )
    .subscribe(symbols => {
      that.symbols = symbols;
    });

    this.insertQueryDictionary = {
      date: 'week',
      symbol_id: 'symbol_id',
      OpenInterest: 'open_interest',
      CommNetto: 'comm_netto',
      CommLong: 'comm_long',
      'CommLong/OI': 'comm_long_oi',
      CommShort: 'comm_short',
      'CommShort/OI': 'comm_short_oi',
      LargeNetto: 'large_netto',
      LargeLong: 'large_long',
      'LargeLong/OI': 'large_long_oi',
      LargeShort: 'large_short',
      'LargeShort/OI': 'large_short_oi',
      SmallNetto: 'small_netto',
      SmallLong: 'small_long',
      'SmallLong/OI': 'small_long_oi',
      SmallShort: 'small_short',
      'SmallShort/OI': 'small_short_oi'
    };
  }

  query$(query, values) {
    return this.connect$().pipe(
      switchMap(connection => {
        return Observable.create(observer => {
          connection.query(query, [values], (err, result) => {
            if (err) {
              console.log(err);
              observer.error(err);
            } else {
              if (result.insertId) {
                observer.next(result.insertId);
              } else if (result.map) {
                result.map(dataPack => observer.next(dataPack))
              } else {
                observer,next(result);
              }
            }
            observer.complete();
          })
        })
      }),
      share()
    )
  };
  
  getSymbols$() {
    return this.query$(`SELECT * FROM symbols`)
    .pipe(
      map(({ name, symbol_id }) => {
        return { name, symbol_id }
      })
    );
  };
  
  addSymbolId$(row) {
    const that = this;
    const foundSymbol = that.symbols.find(v => v.name === row.Name);
    if (!!foundSymbol) {
      if (foundSymbol.observer) {
        return foundSymbol.observer;
      } else {
        const { symbol_id } = foundSymbol;
        return of({ symbol_id, ...row });
      }
    } else {
      const observer = this.query$(`INSERT INTO symbols (name) VALUES ("${row.Name}")`).pipe(
        map(symbol_id => {
          const foundSymbolIndex = that.symbols.findIndex(v => v.name === row.Name);
          const symbol = { name: row.Name, symbol_id };
          if (foundSymbolIndex > -1) {
            that.symbols[foundSymbolIndex] = symbol;
          } else {
            that.symbols.push(symbol);
          }
          return { symbol_id, ...row };
        })
      );
      const symbol = { name: row.Name, symbol_id: null, observer };
      that.symbols.push(symbol);
      return observer;
    }
  };

  adaptRowForDbInsert$(row) {
    const out = [];
    Object.keys(this.insertQueryDictionary).forEach((key) => {
      if (row[key]) {
        out.push(row[key]);
      }
    });

    return out;
  }

  insertSymbolRowInDb$(rows) {
    const query = "INSERT INTO symbols_data (week, symbol_id, open_interest, comm_netto, comm_long, comm_long_oi, comm_short, comm_short_oi, large_netto, large_long, large_long_oi, large_short, large_short_oi, small_netto, small_long, small_long_oi, small_short, small_short_oi) VALUES ?"
    return this.query$(query, rows);
  }
  
  connect$() {
    return Observable.create((observer) => {
      if (this.connection !== null) {
        observer.next(this.connection);
      } else {
        const connection = this.mysqlConnect();
        const that = this;
        connection.connect(err => {
          if (err) {
            console.log(err);
            observer.error(err);
          } else {
            observer.next(connection);
            that.connection = connection;
          }
          observer.complete();
        });
      }
    })
  };

  mysqlConnect() {
    return mysql.createConnection({
      host: "localhost",
      port: "3309",
      user: "cot",
      password: "cot",
      database: "cot"
    });
  }
}

const r = new Repository();
const excelFiles = [
  {
    name: 'CoT_Daten_2019.xlsx',
    url: 'https://s1ee9bc4c878f2f5f.jimcontent.com/download/version/1556307196/module/7553454581/name/CoT%20Daten%202019.xlsx'
  },
  {
    name: 'CoT_Daten_2010-2017.xlsx',
    url: 'https://s1ee9bc4c878f2f5f.jimcontent.com/download/version/1529697217/module/7339046781/name/CoT%20Daten%202010%20-%202017.xlsx'
  },
  {
    name: 'CoT_Daten_1990-1999.xlsx',
    url: 'https://s1ee9bc4c878f2f5f.jimcontent.com/download/version/1529697250/module/7339047381/name/CoT%20Daten%201990%20-%201999.xlsx'
  },
  {
    name: 'CoT_Daten_2018.xlsx',
    url: 'https://s1ee9bc4c878f2f5f.jimcontent.com/download/version/1550004467/module/7553454681/name/CoT%20Daten%202018%20.xlsx'
  },
  {
    name: 'CoT_Daten_2000-2009.xlsx',
    url: 'https://s1ee9bc4c878f2f5f.jimcontent.com/download/version/1529697228/module/7339046881/name/CoT%20Daten%202000%20-%202009.xlsx'
  },
  {
    name: 'CoT_Daten_1986-1989.xlsx',
    url: 'https://s1ee9bc4c878f2f5f.jimcontent.com/download/version/1529697265/module/7339047581/name/CoT%20Daten%201986%20-%201989.xlsx'
  }
];
const options = {
  method: 'GET',
  encoding: null
};

const requestData$ = url => {
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

const getRows$ = $ => {
  const out = [];
  const rowKeys = [];
  $('tr.head').find('td').each((idx, val) => {
    rowKeys.push($(val).text());
  });

  $('tr.d').each((i, node) => {
    const row = {};
    $(node).find('td').each((idx, val) => {
      let value =  $(val).text().trim();
      if (rowKeys[idx] !== 'Name') {
        value = value.replace(/[^\d.,-]/g, '').replace(",", '.');
        if(value === '-'){
          value = "0";
        }
      }
      row[rowKeys[idx]] = value;
    });
    row.date = moment($('p').text().replace('Stand: ', ''), 'DD-MM-YYYY').toDate();
    out.push(row);
  });

  // return out;
  return from(out);
};

const getUrls = $ => {
  return from($('div.link a')
  .slice(0,25)
  .map((index, link) => {
    return `https://cnt1.suricate-trading.de/cotde/${link.attribs.href}`
  }));
};

const requestDataStream$ = requestData$('https://cnt1.suricate-trading.de/cotde/cot-history.html').pipe(
  map(body => cheerio.load(body)),
  map(html => getUrls(html)),
  switchAll(),
  map(url => {
    return requestData$(url);
  }),
  mergeAll(),
  map(tableBody => cheerio.load(tableBody)),
  flatMap($ => getRows$($)),
  flatMap(row => r.addSymbolId$(row)),
  // tap(row => console.log(row)),
  map(row => r.adaptRowForDbInsert$(row)),
  bufferCount(50),
  flatMap(rows => r.insertSymbolRowInDb$(rows))
  
).subscribe(d => {
  console.log(d, arguments, ' here');
});

// const  symbolsTableStream$ = r.getSymbols$().pipe(
//   scan((acc, value) => { 
//     return [...acc, value];
//   }, [])
// );

// const allRows$ = combineLatest(symbolsTableStream$, requestDataStream$).pipe(
//   map(([symbols, row]) => {
//     return {
//       isFound: !!symbols.find(v => v.name === row.Name),
//       ...row
//     };
//   })
// )

// const foundRows$ = allRows$.pipe(
//   filter(row => row.isFound)
// );

// const notFoundRows$ = allRows$.pipe(
//   filter(row => !row.isFound),
//   flatMap(row => {
//     return r.insertSymbol$(row)
//   })
// )
// .subscribe(a => console.log(a));;


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

