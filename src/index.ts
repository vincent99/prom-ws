import { WebSocketServer } from 'ws';
import { WebSocket } from 'ws'
import fetch from 'cross-fetch';
import dayjs from 'dayjs'
import { randomUUID, sign } from 'crypto';
import aws4 from 'aws4'
import type { Credentials } from 'aws4'

const port = 8080
const server = new WebSocketServer({port})
const api = process.env.API || 'http://localhost:30000'

console.info('Listening on:', port)
console.info('Endpoint:', api)
console.info('Auth:', credentials().accessKeyId ? 'AWS' : 'None')

// ?query=container_cpu_usage_seconds_total&start=2022-11-10T00:00:00Z&end=2022-11-11T4:44:00Z&step=5m
interface Subscription {
  id: string
  query: string
  metrics: string[]
  step: number
  history: number
  timer?: NodeJS.Timeout
  last?: string
}

type Subscriptions = Record<string,Subscription>

interface StartReq {
  type: 'start'
  id: string
  query: string
  metrics?: string[]
  step?: number
  history?: number
}

interface StopReq {
  type: 'stop'
  id: string
}

interface ResetReq {
  type: 'reset'
}

interface Point {
  id: string
  t: number
  k: string
  v: string
  [key: string]: unknown
}

type Req = StartReq | StopReq | ResetReq

server.on('connection', (ws) => {
  const subs: Subscriptions = {}

  ws.on('message', async (data) => {
    const raw = data.toString();
    // console.log('Raw', raw)
    const req = toReq(raw)
    // console.log('Req', req)

    if ( !req ) {
      return
    }

    if ( req.type === 'start' ) {
      const sub = toSub(req)

      if ( subs[sub.id] ) {
        console.info('Already subscribed', req)
      } else {
        // console.info('Adding subscribe for', req)
        start(ws, sub, subs)
      }
    } else if ( req.type === 'stop' ) {
      stop(req.id, subs)
    } else if ( req.type === 'reset' ) {
      cleanup(subs)
    }
  });

  ws.on('close', () => {
    cleanup(subs)
  })

  ws.on('error', () => {
    cleanup(subs)
  })
})

function toReq(json: string) {
  try {
    return JSON.parse(json) as Req
  } catch (e) {
    // console.error(e)
  }
}

function toSub(req: StartReq) {
  const out = {
    id: req.id,
    query: req.query,
    metrics: req.metrics || [],
    step: req.step || 5,
    history: req.history || 60,
  } as Subscription

  if ( !out.id ) {
    out.id = randomUUID()
  }

  // if ( !out.metrics.length ) {
  //   out.metrics = ['namespace','deployment','pod']
  // }

  return out
}

async function start(ws: WebSocket, sub: Subscription, subs: Subscriptions) {
  const now = dayjs();
  const last = await range(ws, sub)
  const elapsed = now.diff(last*1000)
  const delay = Math.max(sub.step*1000 - elapsed, 0) + 400

  subs[sub.id] = sub

  console.info('Adding subscribe for', sub, 'delay', delay)

  // Run once just after the data should have updated
  sub.timer = setTimeout(() => {
    poll(ws, sub)

    // Then repeatedly every step seconds
    sub.timer = setInterval(() => {
      poll(ws, sub)
    }, sub.step*1000);
  }, delay);
}

function stop(id: string, subs: Subscriptions) {
  const sub = subs[id]
  if ( sub ) {
    clearInterval(sub.timer);
    delete subs[sub.id]
  }
}

function cleanup(subs: Subscriptions) {
  for ( const k in subs ) {
    stop(k, subs)
  }
}

async function range(ws: WebSocket, sub: Subscription): Promise<number> {
  const url = new URL(`${api}/api/v1/query_range`)
  url.searchParams.set('query', sub.query)

  const now = dayjs();
  const start = now.subtract(sub.history||0, 'second')
  url.searchParams.set('start', start.toISOString())
  url.searchParams.set('end', now.toISOString())
  url.searchParams.set('step', `${sub.step}s`)


  const res = await request(url)
  const body = await res.json() as any

  // console.log('Range Data', res.status, res.headers, body)
  let lastTimeStamp = -1

  for ( const row of body.data.result ) {
    // console.log('Range Row', row)
    for ( const val of row.values ) {
      if ( val[0] > lastTimeStamp ) {
        lastTimeStamp = val[0]
      }

      emit(val, row, sub, ws)
    }
  }

  return lastTimeStamp
}

async function poll(ws: WebSocket, sub: Subscription) {
  const url = new URL(`${api}/api/v1/query`)
  url.searchParams.set('query', sub.query)

  const res = await request(url)
  const body = await res.json() as any
  // console.log('Poll Data', body)

  for ( const row of body.data.result ) {
    emit(row.value, row, sub, ws)
  }
}

function emit(val: [number,string], row: any, sub: Subscription, ws: WebSocket) {
  const key = row.metric.id || `${row.metric.namespace}/${row.metric.pod}`
  const entry: Point = {id: sub.id, k: key, t: val[0], v: val[1]}

  for ( const k of sub.metrics ) {
    entry[k] = row.metric[k]
  }

  ws.send(JSON.stringify(entry))
}

function credentials(): Credentials {
  const accessKeyId = process.env.AWS_ACCESS_KEY_ID || ''
  const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY || ''
  const sessionToken = process.env.AWS_SESSION_TOKEN || ''

  // @TODO read short term keys here
  return {
    accessKeyId,
    secretAccessKey,
    sessionToken,
  }
}

async function request(url: URL, opt?: RequestInit) {
  // console.info('GET', url.toString())

  if ( !opt ) {
    opt = {}
  }

  const headers: Record<string,string> = {}
  headers['User-Agent'] = 'prom-ws'

  const creds = credentials()

  if ( creds.accessKeyId ) {
    const signable = {
      method: 'GET',
      host: url.host,
      path: url.pathname + '?' + url.searchParams.toString(),
      body: opt.body as string|undefined,
      headers: headers,
      service: 'aps',
    };

    aws4.sign(signable, credentials());

    opt.headers = headers

    for ( const k in signable.headers ) {
      headers[k] = signable.headers[k] as string
    }
  }

  return fetch(url.toString(), opt)
}
