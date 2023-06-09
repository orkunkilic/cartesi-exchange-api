import fastify from 'fastify'
const fastifySqlite = require('fastify-sqlite')
import { hex2str } from './utils'
import cors from '@fastify/cors'

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

const pollGraph = async (db: any) => {

    try {
        const cursor = (await db.all('SELECT last FROM cursor'))[0].last || 0
        console.log("Cursor: ", cursor)

        const res = await (await fetch('http://localhost:4000/graphql', {
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
        })).json()
        const reports = res.data.reports.nodes
        const outputs = reports
            .sort((a: any, b: any) => {
                // sort by epoch index and then by input index
                const epochResult = a.input.epoch.index - b.input.epoch.index;
                if (epochResult != 0) {
                    return epochResult;
                } else {
                    return a.input.index - b.input.index;
                }
            })
            .map((n: any) => {
                const output: any = {};
                output.id = n.id;
                output.epoch = n.input.epoch.index;
                output.input = n.input.index;
                output.report = n.index;
                output.payload = hex2str(n.payload);
                return output;
            });

        const payloads = outputs.map((output: any) => JSON.parse(output.payload))
        if(payloads.length === 0) {
            console.log('nothing to set')
            return
        }
        const insertPromises = payloads.map((event: any) => {
            switch (event.type) {
                case "ADD_ORDER": {
                    console.log("add order", event)
                    const table = event.order_type === "bid" ? "bids" : "asks"
                    return db.run(`INSERT INTO ${table} (id, owner, price, quantity) VALUES (?, ?, ?, ?)`, [event.order_id, event.order_owner.toLowerCase(), event.order_price, event.order_quantity])
                }
                case "REMOVE_ORDER": {
                    console.log("remove order", event)
                    const table = event.order_type === "bid" ? "bids" : "asks"
                    return db.run(`DELETE FROM ${table} WHERE id=?`, [event.order_id])
                }
                case "BALANCE_UPDATED":
                    // TODO: make it upsert
                    return db.run('UPDATE balances SET total=?, available=? WHERE address=? AND token=?', [event.total, event.available, event.user, event.token])
                default:
                    break;
            }
        })

        console.log('Setting last fetched index: ', outputs[outputs.length - 1].id)
        insertPromises.push(
            db.run('UPDATE cursor SET last=?', [outputs[outputs.length - 1].id])
        )

        console.log('Promises to run')
        try {
            await Promise.all(insertPromises)
        } catch (e) {
            console.log("Error while inserting", e)
        }
        console.log('Promises runned')
    } catch (err) {
        console.log('Error while fetching and setting from graph: ', err)
    }
}

const server = fastify()

server.register(cors, {
    origin: '*'
})

server.register(fastifySqlite, {
    promiseApi: true,
    dbFile: 'exchange.db'
})

server.get('/asks', async (request: any, reply) => {
    if (!request.query.address) {
        const asks = await server.sqlite.all('select * from asks order by price asc ')
        return asks
    } else {
        const asks = await server.sqlite.all('select * from asks order by price asc where owner = ?', [request.query?.address.toLowerCase()])
        return asks
    }
})

server.get('/bids', async (request:any, reply) => {
    if (!request.query.address) {
        const bids = await server.sqlite.all('select * from bids order by price desc')
        return bids
    } else {
        const bids = await server.sqlite.all('select * from bids order by price desc where owner = ?', [request.query?.address.toLowerCase()])
        return bids
    }
})

server.get('/balance', async (request: any, reply) => {
    const balance = await server.sqlite.all('select * from balances where address = ?', [request.query?.address.toLowerCase()]) || {}
    return balance
})

server.listen({ port: 8080, host: "0.0.0.0" }, async (err, address) => {
    if (err) {
        console.error(err)
        process.exit(1)
    }
    console.log(`Server listening at ${address}`)

    try {
        await server.sqlite.run('SELECT * FROM balances')
    } catch {
        // create tables
        console.log('started creating tables')

        await server.sqlite.run('CREATE TABLE balances (address varchar(255) NOT NULL, token varchar(255) NOT NULL, total int, available int, PRIMARY KEY (address, token))')

        // TODO: fix this demo only thing
        await server.sqlite.run('INSERT INTO balances (address, token) VALUES (?, ?)', ["0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266", "ask"])
        await server.sqlite.run('INSERT INTO balances (address, token) VALUES (?, ?)', ["0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266", "bid"])

        await server.sqlite.run('CREATE TABLE asks (id int NOT NULL, owner varchar(255) NOT NULL, price int NOT NULL, quantity int NOT NULL, PRIMARY KEY (id))')
        await server.sqlite.run('CREATE TABLE bids (id int NOT NULL, owner varchar(255) NOT NULL, price int NOT NULL, quantity int NOT NULL, PRIMARY KEY (id))')

        await server.sqlite.run('CREATE TABLE cursor (last int)')
        await server.sqlite.run('INSERT INTO cursor (last) VALUES (0)')

        console.log('tables created')
    }

    setInterval(() => {
        pollGraph(server.sqlite)
    }, 3000)

})