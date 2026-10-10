# rust-sqlite-multiwriter

Varios escritores concurrentes sobre **una** base de datos SQLite, en Rust.

Port a Rust del motor de [sqliteai/sqlite-multiwriter](https://github.com/sqliteai/sqlite-multiwriter)
(artículo: [We solved SQLite's single-writer limitation](https://marcobambini.substack.com/p/we-solved-sqlites-single-writer-limitation)),
hermano de [go-sqlite-multiwriter](https://github.com/digiogithub/go-sqlite-multiwriter). Es un **VFS**
registrado en el SQLite de `libsqlite3-sys` (el mismo que usa `rusqlite` con `bundled`): tu SQL, tu esquema y tu
fichero `.db` no cambian, y el fichero vuelve a ser una base SQLite normal al cerrar la última conexión.

```rust
use sqlite_multiwriter::{open, run_tx, Options};

// una conexión por hilo: cada una es un escritor independiente
let mut conn = open("app.db", &Options { rebase: true, ..Default::default() })?;
run_tx(&mut conn, |tx| {
    tx.execute("UPDATE account SET balance = balance + 10 WHERE id = ?1", [7])
})?; // repite la transacción entera si pierde contra otro commit (SQLITE_BUSY_SNAPSHOT)
```

Sin el feature `rusqlite` (activo por defecto) basta con `sqlite_multiwriter::register()` y abrir la URI
`file:app.db?vfs=multiwriter` con `SQLITE_OPEN_URI`.

## Cómo funciona

```
escritor 1 --> WAL privado --+
escritor 2 --> WAL privado --+--> validación y publicación --> log de commits --> fichero .db
escritor 3 --> WAL privado --+    (gana el primero en confirmar)                 (compactación)
```

* **Carriles privados**: cada conexión tiene su WAL en memoria y su wal-index propio; nadie espera por el lock
  de escritura. Las escrituras al `.db` se descartan: sólo el compactador lo escribe.
* **Snapshots y versiones de página**: almacén `(página, época)` con locks por franjas; cada transacción lee su
  instantánea.
* **Commit**: se valida el *schema cookie*, las páginas **leídas** (evita *write skew*; se perdona una página
  interior de tabla cuyo enrutado no cambió) y las **escritas** (gana el primero). Si falla:
  `SQLITE_BUSY_SNAPSHOT`.
* **Reubicación** de páginas nuevas cuando varios commits hacen crecer el fichero a la vez, y **fusión** a tres
  bandas de páginas interiores.
* **Rebase** (`Options::rebase`): un commit que sólo perdió por compartir página con otras filas se re-ejecuta
  fila a fila (decodificando las páginas) sobre el estado más reciente, en lotes, en una conexión auxiliar.
* **Durabilidad**: log segmentado `<db>-mw*` con *group commit* (`synchronous=FULL`), recuperación tras crash
  y compactación en segundo plano.
* **Barrera DDL** vía `sqlite3_trace_v2` (instalado con `sqlite3_auto_extension`).

## Estado

| | |
|---|---|
| Carriles privados, snapshots, validación, reubicación, merge, log, recuperación, compactación, GC, barrera DDL | ✅ |
| Rebase (`mw_rebase=1`) | ✅ |
| Multiproceso (`mw_mp=1`) | ⏳ (hoy un proceso por base; un segundo proceso recibe `SQLITE_BUSY`) |

## Rendimiento

`cargo run --release --example mwbench -- bulk|groups|hot 1,4,16 4` — 4 núcleos, disco local,
`synchronous=FULL`. `sqlite` = SQLite de serie en WAL con `BEGIN IMMEDIATE`; `mw` = motor; `mwr` = motor con rebase.

| carga | escritores | SQLite tx/s | mw tx/s | mwr tx/s | SQLite p99.9 | mw/mwr p99.9 |
|---|---:|---:|---:|---:|---:|---:|
| bulk (100 inserts/tx, tabla propia) | 16 | 3.451 | **4.970** | 4.985 | 1.466 ms | 47 ms |
| groups (cada uno su fila, misma página) | 16 | 5.935 | 3.194 (215 reint./100) | **11.849** (0 reint.) | 54 ms | **6,8 ms** |
| hot (4 filas compartidas: conflicto real) | 16 | **6.066** | 3.388 | 3.412 | 330 ms | 54 ms |

## Opciones (URI)

`vfs=multiwriter` (o `mw=1`) activa el motor; `mw_rebase=1`, `mw_rebase_backoff=0`, `mw_readcheck=0`,
`mw_log_max_mb`, `mw_base_cache_mb`, `mw_gc`, `mw_noreloc`, `mw_nomerge`, `mw_noroute`. `MW_DEBUG=1` explica
cada rechazo del rebase.

## Límites

Sólo WAL (no `locking_mode=EXCLUSIVE` ni `auto_vacuum`), sistema de ficheros local, la base sólo se usa a
través del motor mientras está abierta, y una transacción rechazada se repite entera. El rebase sólo re-ejecuta
transacciones "a ciegas" (`INSERT ... VALUES`, `UPDATE`/`DELETE` por clave, sin `SELECT` previo) y no aplica con
triggers, tablas virtuales o `AUTOINCREMENT`.

## Tests

```
cargo test --release      # concurrencia, invariantes, write skew, DDL, crash/recuperación, rebase
```

Licencia Apache 2.0 (ver `LICENSE` y `NOTICE`).
