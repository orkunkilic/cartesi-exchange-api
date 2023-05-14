"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const fastify_1 = __importDefault(require("fastify"));
const fastifySqlite = require('fastify-sqlite');
const utils_1 = require("./utils");
const cors_1 = __importDefault(require("@fastify/cors"));
const GET_REPORTS = `
query reports($after: String!) {
    reports(after: $after) {
      nodes {
        id
        index
        payload
        input {
          index
          epoch {
            index
          }
        }
      }
    }
  }
`;
const pollGraph = (db) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const cursor = (yield db.all('SELECT last FROM cursor'))[0].last || 0;
        console.log("Cursor: ", cursor);
        const res = yield (yield fetch('http://localhost:4000/graphql', {
            method: 'POST',
            body: JSON.stringify({
                query: GET_REPORTS,
                variables: {
                    after: cursor.toString()
                }
            }),
            headers: {
                "Content-Type": "application/json",
            }
        })).json();
        const reports = res.data.reports.nodes;
        const outputs = reports
            .sort((a, b) => {
            // sort by epoch index and then by input index
            const epochResult = a.input.epoch.index - b.input.epoch.index;
            if (epochResult != 0) {
                return epochResult;
            }
            else {
                return a.input.index - b.input.index;
            }
        })
            .map((n) => {
            const output = {};
            output.id = n.id;
            output.epoch = n.input.epoch.index;
            output.input = n.input.index;
            output.report = n.index;
            output.payload = (0, utils_1.hex2str)(n.payload);
            return output;
        });
        const payloads = outputs.map((output) => JSON.parse(output.payload));
        if (payloads.length === 0) {
            console.log('nothing to set');
            return;
        }
        const insertPromises = payloads.map((event) => {
            switch (event.type) {
                case "ADD_ORDER": {
                    console.log("add order", event);
                    const table = event.order_type === "bid" ? "bids" : "asks";
                    return db.run(`INSERT INTO ${table} (id, owner, price, quantity) VALUES (?, ?, ?, ?)`, [event.order_id, event.order_owner.toLowerCase(), event.order_price, event.order_quantity]);
                }
                case "REMOVE_ORDER": {
                    console.log("remove order", event);
                    const table = event.order_type === "bid" ? "bids" : "asks";
                    return db.run(`DELETE FROM ${table} WHERE id=?`, [event.order_id]);
                }
                case "BALANCE_UPDATED":
                    // TODO: make it upsert
                    return db.run('UPDATE balances SET total=?, available=? WHERE address=? AND token=?', [event.total, event.available, event.user, event.token]);
                default:
                    break;
            }
        });
        console.log('Setting last fetched index: ', outputs[outputs.length - 1].id);
        insertPromises.push(db.run('UPDATE cursor SET last=?', [outputs[outputs.length - 1].id]));
        console.log('Promises to run');
        try {
            yield Promise.all(insertPromises);
        }
        catch (e) {
            console.log("Error while inserting", e);
        }
        console.log('Promises runned');
    }
    catch (err) {
        console.log('Error while fetching and setting from graph: ', err);
    }
});
const server = (0, fastify_1.default)();
server.register(cors_1.default, {
    origin: '*'
});
server.register(fastifySqlite, {
    promiseApi: true,
    dbFile: 'exchange.db'
});
server.get('/asks', (request, reply) => __awaiter(void 0, void 0, void 0, function* () {
    var _a;
    if (!request.query.address) {
        const asks = yield server.sqlite.all('select * from asks order by price asc ');
        return asks;
    }
    else {
        const asks = yield server.sqlite.all('select * from asks order by price asc where owner = ?', [(_a = request.query) === null || _a === void 0 ? void 0 : _a.address.toLowerCase()]);
        return asks;
    }
}));
server.get('/bids', (request, reply) => __awaiter(void 0, void 0, void 0, function* () {
    var _b;
    if (!request.query.address) {
        const bids = yield server.sqlite.all('select * from bids order by price desc');
        return bids;
    }
    else {
        const bids = yield server.sqlite.all('select * from bids order by price desc where owner = ?', [(_b = request.query) === null || _b === void 0 ? void 0 : _b.address.toLowerCase()]);
        return bids;
    }
}));
server.get('/balance', (request, reply) => __awaiter(void 0, void 0, void 0, function* () {
    var _c;
    const balance = (yield server.sqlite.all('select * from balances where address = ?', [(_c = request.query) === null || _c === void 0 ? void 0 : _c.address.toLowerCase()])) || {};
    return balance;
}));
server.listen({ port: 8080, host: "0.0.0.0" }, (err, address) => __awaiter(void 0, void 0, void 0, function* () {
    if (err) {
        console.error(err);
        process.exit(1);
    }
    console.log(`Server listening at ${address}`);
    try {
        yield server.sqlite.run('SELECT * FROM balances');
    }
    catch (_d) {
        // create tables
        console.log('started creating tables');
        yield server.sqlite.run('CREATE TABLE balances (address varchar(255) NOT NULL, token varchar(255) NOT NULL, total int, available int, PRIMARY KEY (address, token))');
        // TODO: fix this demo only thing
        yield server.sqlite.run('INSERT INTO balances (address, token) VALUES (?, ?)', ["0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266", "ask"]);
        yield server.sqlite.run('INSERT INTO balances (address, token) VALUES (?, ?)', ["0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266", "bid"]);
        yield server.sqlite.run('CREATE TABLE asks (id int NOT NULL, owner varchar(255) NOT NULL, price int NOT NULL, quantity int NOT NULL, PRIMARY KEY (id))');
        yield server.sqlite.run('CREATE TABLE bids (id int NOT NULL, owner varchar(255) NOT NULL, price int NOT NULL, quantity int NOT NULL, PRIMARY KEY (id))');
        yield server.sqlite.run('CREATE TABLE cursor (last int)');
        yield server.sqlite.run('INSERT INTO cursor (last) VALUES (0)');
        console.log('tables created');
    }
    setInterval(() => {
        pollGraph(server.sqlite);
    }, 3000);
}));
