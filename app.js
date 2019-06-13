const XLSX = require('xlsx');
const request = require('request');
const mysql = require('mysql');
const moment = require('moment');
const { Observable, from, merge, combineLatest } = require('rxjs');
const { filter, reduce, concat, concatAll, mergeMap, map, switchAll, mergeAll, switchMap, mapTo } = require('rxjs/operators');
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

const htmlUrls = [
  'https://cnt1.suricate-trading.de/cotde/history/cot-2019-04-30.html'
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

//week, symbol_id, open_interest, comm_netto, comm_long, comm_long_oi, comm_short, comm_short_oi, large_netto, large_long,
// large_long_oi, large_short, large_short_oi, small_netto, small_long, small_long_oi, small_short, small_short_oi;

// document.querySelectorAll("div.link")

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

requestData('https://cnt1.suricate-trading.de/cotde/cot-history.html').pipe(
  map(body => cheerio.load(body)),
  map(html => getUrls(html)),
  switchAll(),
  map(url => {
    return requestData(url);
  }),
  mergeAll(),
  map(tableBody => cheerio.load(tableBody)),
  map($ => getRows($))
).subscribe((data) => {
  console.log(data);
});

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

const query = (connection) => (query) => {
  return Observable.create(observer => {
    connection.query(query, (err, result) => {
      if (err) {
        observer.error(err);
      } else {
        result.map(dataPack => observer.next(dataPack))
      }
    });
  });
};

const getSymbols = connection => query(connection)(`SELECT * FROM symbols`).pipe(
  map(({ name, symbol_id }) => {
    return { name, symbol_id };
  })
);

connect().pipe(
  map(connection => getSymbols(connection)),
  switchAll()
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

const requestDataProc = () => {
  return;
  request(htmlUrls[0], (err, res, body) => {
    const $ = cheerio.load(body);
    const rows = $('tr.d');
    const date = moment($('p').text().replace('Stand: ', ''), 'DD-MM-YYYY').toDate();
    const out = [];

    rows.each(function(i, row) {
      if (row.name === 'tr') {
        const rowArr = [];
        rowArr.push(date);
        if (row.children) {
          row.children.forEach((node, idx) => {
            if (node.type === 'tag' && node.name === 'td') {
              // TODO not the best way have to think of something else
              if (idx === 1) {
                const symbol = symbols.find((sym) => {
                  return sym.name === $(node).text();
                });

                if (symbol && symbol.symbol_id) {
                  rowArr.push(symbol.symbol_id);
                } else {
                  // TODO create the symbol in the DB and get its ID
                }
              } else {
                // TODO remove unused chars from the string (%)
                rowArr.push($(node).text())
              }
            }
          });
        }
        out.push(rowArr);
      }
    });

    // console.log(out);
  })
};

const createConnection = (query) => {
  return;
  con.connect(function (err) {
    // TODO check if there is an open connection and use it, open only if there is no connection
    if (err) {
      query();
      // throw err;
    }
    console.log('Connected');
    query();
  });
};

function init() {
  return;
  const symbolsPromise = () => {
    return new Promise((resolve, reject) => {
      createConnection(() => {
        con.query(`SELECT * FROM symbols`, function (err, result) {
          resolve(result.map(symbol => symbol));
        })
      });
    })
  };

  const symbolsObservable = from(new Promise((resolve, reject) => {
    createConnection(() => {
      con.query(`SELECT * FROM symbols`, function (err, result) {
        resolve(result.map(symbol => symbol));
      })
    });
  }));

  const excelDataObservable = Observable.create((observer) => {
    excelFiles.forEach( (fileInfo) => {
      request({...options, url: fileInfo.url}, (err, res, body) => {
        const workbook = XLSX.read(body, {type: 'buffer'});

        workbook.SheetNames.forEach((workSheetName, index) => {
          const sheetName = workbook.SheetNames[index];
          const workSheet = workbook.Sheets[sheetName];
          const excelData = (XLSX.utils.sheet_to_json(workSheet, {header: "A", blankrows: false}));
          observer.next(excelData);
        });
      })
    });
  });
  // const excelDataSub = excelDataObservable.subscribe((data) => {
  //   data.forEach((row) => {
  //     console.log(getWeeksKeys(row, data[0]));
  //   })
  // });

  const symbolsSyb = symbolsObservable.subscribe((val) => {
    symbols = val;
  });
  const combined = excelDataObservable.pipe(mergeMap((excelData) => {
    excelData.forEach((row, idx) => {
      if (idx !== 0 && idx !== 1) {
        console.log(getWeeksKeys(excelData[0], row));
      }
    });
  
    return symbolsPromise().then((res) => {
      return {
        data: excelData,
        symbols: res
      }
    })
  }));
  
  const subscribe = combined.subscribe((combinedVal) => {
    // console.log(combinedVal);
  });
}

// TODO use merge to connect the excel stream and the html stream

// con.connect(function (err) {
//   if (err) {
//     throw err;
//   }
//   console.log('Connected');
//
//   excelFiles.forEach((fileInfo) => {
//     request({...options, url: fileInfo.url}, (err, res, body) => {
//       const workbook = XLSX.read(body, {type: 'buffer'});
//
//       workbook.SheetNames.forEach((workSheetName, index) => {
//         const sheetName = workbook.SheetNames[index];
//         const workSheet = workbook.Sheets[sheetName];
//         const excelData = (XLSX.utils.sheet_to_json(workSheet, {header: "A", blankrows: false}));
//         const weeks = excelData[0];
//
//         excelData.forEach(async (row, index) => {
//           if (index !== 0 && index !== 1) {
//             // TODO make this an observable in order to get filteredRowDataKeys
//             await con.query(`SELECT * FROM symbols where name = "${row['A']}"`, function(err, result) {
//               // TODO if the symbol does not exists create it
//               if (err) {
//                 throw err;
//               }
//               const { symbol_id } = result[0];
//               const filteredRowDataKeys = Object.keys(row).filter((key) => {
//                 return result[0].name !== row[key];
//               });
//               let out = [];
//               const weeksKeys = getWeeksKeys(filteredRowDataKeys, weeks);
//               Object.keys(weeksKeys).forEach(async(week, idx) => {
//                 out.push([
//                   moment(week.replace('Stand: ', ''), 'DD-MM-YYYY').toDate(),
//                   symbol_id,
//                   row[weeksKeys[week][0]],
//                   row[weeksKeys[week][1]],
//                   row[weeksKeys[week][2]],
//                   row[weeksKeys[week][3]],
//                   row[weeksKeys[week][4]],
//                   row[weeksKeys[week][5]],
//                   row[weeksKeys[week][6]],
//                   row[weeksKeys[week][7]],
//                   row[weeksKeys[week][8]],
//                   row[weeksKeys[week][9]],
//                   row[weeksKeys[week][10]],
//                   row[weeksKeys[week][11]],
//                   row[weeksKeys[week][12]],
//                   row[weeksKeys[week][13]],
//                   row[weeksKeys[week][14]],
//                   row[weeksKeys[week][15]]
//                 ]);
//
//                 if (!(idx % 3) && idx) {
//                   // console.log(index);
//                   const sql = "INSERT INTO symbols_data (week, symbol_id, open_interest, comm_netto, comm_long, comm_long_oi, comm_short, comm_short_oi, large_netto, large_long, large_long_oi, large_short, large_short_oi, small_netto, small_long, small_long_oi, small_short, small_short_oi) VALUES ?";
//                   con.query(sql, [out], await function (err) {
//                     if (err) {
//                       throw err;
//                     }
//
//                     out = [];
//                   });
//                 }
//               });
//             });
//           }
//         });
//
//       });
//     });
//   });
// });
