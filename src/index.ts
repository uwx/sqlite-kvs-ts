// https://github.com/kujirahand/node-sqlite-kvs

import { DatabaseSync, type StatementSync } from 'node:sqlite';

interface Model { key: string, value: string, ctime: number, mtime: number }

export class KVS<K extends string, V> {
    private readonly db: DatabaseSync | null;
    private readonly stmt_insert: StatementSync;
    private readonly stmt_get: StatementSync;
    private readonly stmt_update: StatementSync;
    private readonly stmt_all: StatementSync;
    private readonly stmt_find: StatementSync;
    private readonly stmt_delete: StatementSync;

    constructor(private readonly dbpath = './kvs.db', tableName = 'items') {
        this.db = new DatabaseSync(this.dbpath);
        this.db.exec(`PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS ${tableName}(key TEXT PRIMARY KEY, value TEXT, ctime INTEGER, mtime INTEGER)`);
        this.stmt_get = this.db.prepare(`SELECT * FROM ${tableName} WHERE key=? LIMIT 1`);
        this.stmt_insert = this.db.prepare(`INSERT INTO ${tableName} (key,value,ctime,mtime) VALUES (?,?,?,?)`);
        this.stmt_update = this.db.prepare(`UPDATE ${tableName} SET value=?, mtime=? WHERE key=?`);
        this.stmt_all = this.db.prepare(`SELECT * FROM ${tableName}`);
        this.stmt_find = this.db.prepare(`SELECT * FROM ${tableName} WHERE key LIKE ?`);
        this.stmt_delete = this.db.prepare(`DELETE FROM ${tableName} WHERE key = ?`);
    }
    //
    // close database
    close() {
        if (this.db) {
            this.db.close();
        }
    };

    get(key: K) {
        const row = this.stmt_get.get(key) as Model;
        if (!row || !row.value) {
            return undefined;
        }
        return JSON.parse(row.value);
    }

    set(key: K, value: V) {
        const t = Date.now();
        const value_p = JSON.stringify(value);
        const update = this.get(key) as (Model | undefined);
        if (update) {
            //
            // update content ==> value=?, mtime=?, key=?'
            this.stmt_update.run(value_p, t, key);
            return this;
        }

        //
        // initial input ==> key,value,ctime,mtime
        this.stmt_insert.run(key, value_p, t, t);
        return this;
    }

    delete(key: K) {
        return this.stmt_delete.run(key).changes > 0;
    }

    all() {
        const rows = this.stmt_all.all() as Model[];
        return this._rows2obj(rows);
    }

    find(prefix: string) {
        const rows = this.stmt_find.all(`${prefix}%`) as Model[];
        return this._rows2obj(rows);
    }

    private _rows2obj(rows: Model[]): Record<K, V> {
        const r = {};
        for (const row of rows) {
            const key = row.key;
            r[key] = JSON.parse(row.value);
        }
        return r as Record<K, V>;
    };
}
