import fs from 'node:fs';
import path from 'node:path';
import { createClient } from '@supabase/supabase-js';

/**
 * Sincroniza datos y archivos de Storage desde la base de datos secundaria (respaldo)
 * hacia la base de datos original (principal) SIN duplicar registros existentes.
 *
 * Uso (desde la raíz del proyecto):
 *   1. Simulación (NO escribe nada en la BD original, solo muestra qué se copiaría):
 *      node supabase/sync_from_backup.mjs
 *
 *   2. Ejecución real (inserta solo registros nuevos y copia archivos faltantes):
 *      node supabase/sync_from_backup.mjs --apply
 *
 *   3. Opcional: actualizar también registros ya existentes si en el respaldo son más recientes:
 *      node supabase/sync_from_backup.mjs --apply --update-newer
 *
 * Variables de entorno requeridas:
 *   SOURCE_SUPABASE_URL="https://rnkisljndrunbbuavwgl.supabase.co"
 *   SOURCE_SUPABASE_KEY="tu_service_role_key_del_respaldo"
 *   TARGET_SUPABASE_URL="https://tu_proyecto_original.supabase.co"
 *   TARGET_SUPABASE_KEY="tu_service_role_key_de_la_original"
 */

const args = new Set(process.argv.slice(2));
const APPLY = args.has('--apply');
const UPDATE_NEWER = args.has('--update-newer');
const SKIP_STORAGE = args.has('--skip-storage');
const EXPORT_ONLY = args.has('--export-only');

const SOURCE_URL = (process.env.SOURCE_SUPABASE_URL || '').trim().replace(/\/+$/, '');
const SOURCE_KEY = (process.env.SOURCE_SUPABASE_KEY || '').trim();
const TARGET_URL = (process.env.TARGET_SUPABASE_URL || '').trim().replace(/\/+$/, '');
const TARGET_KEY = (process.env.TARGET_SUPABASE_KEY || '').trim();

if (!SOURCE_URL || !SOURCE_KEY) {
  console.error('❌ Faltan SOURCE_SUPABASE_URL o SOURCE_SUPABASE_KEY.');
  process.exit(1);
}
if (!EXPORT_ONLY && (!TARGET_URL || !TARGET_KEY)) {
  console.error('❌ Faltan TARGET_SUPABASE_URL o TARGET_SUPABASE_KEY.');
  process.exit(1);
}
if (!EXPORT_ONLY && SOURCE_URL === TARGET_URL) {
  console.error('❌ SOURCE_SUPABASE_URL y TARGET_SUPABASE_URL son iguales. Deben ser proyectos distintos.');
  process.exit(1);
}

const sourceHost = SOURCE_URL.replace(/^https?:\/\//i, '');
const targetHost = TARGET_URL.replace(/^https?:\/\//i, '');

const source = createClient(SOURCE_URL, SOURCE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});
const target = EXPORT_ONLY
  ? null
  : createClient(TARGET_URL, TARGET_KEY, {
      auth: { persistSession: false, autoRefreshToken: false },
    });

const cleanDoc = (val) => String(val || '').replace(/\D/g, '').trim();
const normText = (val) => String(val || '').trim().toLowerCase();

function rewriteUrlsDeep(value) {
  if (!targetHost || sourceHost === targetHost) return value;
  if (typeof value === 'string') {
    return value.includes(sourceHost) ? value.split(sourceHost).join(targetHost) : value;
  }
  if (Array.isArray(value)) {
    return value.map(rewriteUrlsDeep);
  }
  if (value && typeof value === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      out[k] = rewriteUrlsDeep(v);
    }
    return out;
  }
  return value;
}

async function fetchAllRows(client, tableName) {
  const pageSize = 1000;
  let from = 0;
  const all = [];
  while (true) {
    const { data, error } = await client
      .from(tableName)
      .select('*')
      .range(from, from + pageSize - 1);

    if (error) {
      if (error.code === '42P01' || error.message?.includes('does not exist')) {
        console.warn(`⚠️ Tabla '${tableName}' no existe en este proyecto. Se omite.`);
        return [];
      }
      throw new Error(`Error leyendo ${tableName}: ${error.message}`);
    }
    if (!data || data.length === 0) break;
    all.push(...data);
    if (data.length < pageSize) break;
    from += pageSize;
  }
  return all;
}

async function safeInsert(client, tableName, row) {
  const payload = { ...row };
  for (let attempt = 0; attempt < 8; attempt++) {
    const { data, error } = await client
      .from(tableName)
      .insert([payload])
      .select('*')
      .maybeSingle();

    if (!error) return { data, error: null };

    const msg = error.message || '';
    const colMatch = msg.match(/Could not find the '([^']+)' column/i);
    if ((error.code === 'PGRST204' || msg.includes('Could not find the')) && colMatch?.[1]) {
      delete payload[colMatch[1]];
      continue;
    }
    return { data: null, error };
  }
  return { data: null, error: new Error(`Demasiados reintentos insertando en ${tableName}`) };
}

async function safeUpdate(client, tableName, id, row) {
  const payload = { ...row };
  delete payload.id;
  for (let attempt = 0; attempt < 8; attempt++) {
    const { data, error } = await client
      .from(tableName)
      .update(payload)
      .eq('id', id)
      .select('*')
      .maybeSingle();

    if (!error) return { data, error: null };

    const msg = error.message || '';
    const colMatch = msg.match(/Could not find the '([^']+)' column/i);
    if ((error.code === 'PGRST204' || msg.includes('Could not find the')) && colMatch?.[1]) {
      delete payload[colMatch[1]];
      continue;
    }
    return { data: null, error };
  }
  return { data: null, error: new Error(`Demasiados reintentos actualizando ${tableName}`) };
}

function isSourceNewer(srcRow, tgtRow) {
  const srcDate = new Date(
    srcRow.updated_at || srcRow.fecha_actualizacion || srcRow.created_at || srcRow.fecha_creacion || 0
  ).getTime();
  const tgtDate = new Date(
    tgtRow.updated_at || tgtRow.fecha_actualizacion || tgtRow.created_at || tgtRow.fecha_creacion || 0
  ).getTime();
  return Number.isFinite(srcDate) && Number.isFinite(tgtDate) && srcDate > tgtDate;
}

async function listBucketFilesRecursively(client, bucketName, prefix = '') {
  const files = [];
  let offset = 0;
  const limit = 1000;

  while (true) {
    const { data, error } = await client.storage.from(bucketName).list(prefix, { limit, offset });
    if (error) {
      console.warn(`⚠️ No se pudo listar bucket '${bucketName}/${prefix}': ${error.message}`);
      break;
    }
    if (!data || data.length === 0) break;

    for (const item of data) {
      if (!item.name || item.name === '.emptyFolderPlaceholder') continue;
      const fullPath = prefix ? `${prefix}/${item.name}` : item.name;
      if (item.id === null) {
        const nested = await listBucketFilesRecursively(client, bucketName, fullPath);
        files.push(...nested);
      } else {
        files.push({
          path: fullPath,
          mimeType: item.metadata?.mimetype || undefined,
        });
      }
    }

    if (data.length < limit) break;
    offset += limit;
  }

  return files;
}

async function syncStorageBuckets() {
  const buckets = ['anexos', 'firma'];
  const summary = [];

  for (const bucket of buckets) {
    const srcFiles = await listBucketFilesRecursively(source, bucket);
    const tgtFiles = await listBucketFilesRecursively(target, bucket);
    const tgtSet = new Set(tgtFiles.map((f) => f.path));

    const missing = srcFiles.filter((f) => !tgtSet.has(f.path));
    let copied = 0;
    let errors = 0;

    if (APPLY) {
      for (const file of missing) {
        try {
          const { data: blob, error: dlErr } = await source.storage.from(bucket).download(file.path);
          if (dlErr || !blob) {
            errors++;
            console.warn(`  ❌ Error descargando ${bucket}/${file.path}: ${dlErr?.message}`);
            continue;
          }
          const arrayBuffer = await blob.arrayBuffer();
          const { error: upErr } = await target.storage.from(bucket).upload(file.path, arrayBuffer, {
            upsert: false,
            contentType: file.mimeType || blob.type || 'application/octet-stream',
          });
          if (upErr && !upErr.message?.toLowerCase().includes('already exists')) {
            errors++;
            console.warn(`  ❌ Error subiendo ${bucket}/${file.path}: ${upErr.message}`);
          } else {
            copied++;
          }
        } catch (err) {
          errors++;
          console.warn(`  ❌ Excepción copiando ${bucket}/${file.path}: ${err.message}`);
        }
      }
    }

    summary.push({
      bucket,
      origen: srcFiles.length,
      destinoExistentes: tgtFiles.length,
      nuevosACopiar: missing.length,
      copiados: APPLY ? copied : 0,
      errores: errors,
    });
  }

  return summary;
}

const TABLES = [
  'sec_secretarias',
  'profiles',
  'contratos',
  'informes_mensuales',
  'informe_obligaciones',
  'informe_anexos',
  'certificaciones_supervision',
  'soportes_fiduciaria',
  'declaraciones_renta',
  'autorizaciones_desembolso',
  'notificaciones',
  'configuracion_ia',
  'informes_finales',
];

async function run() {
  console.log('==============================================================');
  console.log(
    EXPORT_ONLY
      ? '📦 MODO SOLO EXPORTACIÓN (Respaldo -> JSON local)'
      : APPLY
      ? `🚀 MODO EJECUCIÓN REAL (${UPDATE_NEWER ? 'Insertar nuevos + Actualizar más recientes' : 'Solo insertar nuevos, sin sobrescribir'})`
      : '🔍 MODO SIMULACIÓN (--dry-run): no se escribirá nada en la BD original'
  );
  console.log(`   Origen (respaldo): ${SOURCE_URL}`);
  if (!EXPORT_ONLY) console.log(`   Destino (original): ${TARGET_URL}`);
  console.log('==============================================================\n');

  // 1. Extraer copia completa de respaldo y guardarla como archivo JSON local de seguridad
  const sourceDump = {};
  for (const table of TABLES) {
    sourceDump[table] = await fetchAllRows(source, table);
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const exportFile = path.resolve('supabase', `backup_export_${stamp}.json`);
  fs.writeFileSync(exportFile, JSON.stringify(sourceDump, null, 2), 'utf8');
  console.log(`💾 Copia de seguridad extraída en: ${exportFile}\n`);

  if (EXPORT_ONLY) {
    for (const table of TABLES) {
      console.log(`  - ${table}: ${sourceDump[table].length} filas`);
    }
    return;
  }

  // 2. Leer estado actual de la BD original
  const targetData = {};
  for (const table of TABLES) {
    targetData[table] = await fetchAllRows(target, table);
  }

  // Mapas de remapeo de IDs (respaldo ID -> original ID)
  const secIdMap = new Map();
  const profileIdMap = new Map();
  const contratoIdMap = new Map();
  const informeIdMap = new Map();
  const existingTargetInformeIds = new Set(targetData.informes_mensuales.map((r) => r.id));

  const stats = [];

  // Helper de sincronización genérica por tabla
  async function syncTable(tableName, findMatch, prepareRow) {
    const srcRows = sourceDump[tableName];
    const tgtRows = targetData[tableName];
    let inserted = 0;
    let updated = 0;
    let skipped = 0;
    let errors = 0;

    for (const rawSrc of srcRows) {
      const prepared = prepareRow(rewriteUrlsDeep(rawSrc));
      if (!prepared) {
        skipped++;
        continue;
      }

      const match = findMatch(prepared, tgtRows);
      if (match) {
        if (UPDATE_NEWER && isSourceNewer(prepared, match)) {
          if (APPLY) {
            const { error } = await safeUpdate(target, tableName, match.id, prepared);
            if (error) {
              errors++;
              console.warn(`  ❌ [${tableName}] Error actualizando ${match.id}: ${error.message}`);
            } else {
              updated++;
            }
          } else {
            updated++;
          }
        } else {
          skipped++;
        }
      } else {
        if (APPLY) {
          const { data, error } = await safeInsert(target, tableName, prepared);
          if (error) {
            errors++;
            console.warn(`  ❌ [${tableName}] Error insertando ${prepared.id || ''}: ${error.message}`);
          } else {
            inserted++;
            if (data) tgtRows.push(data);
          }
        } else {
          inserted++;
          tgtRows.push(prepared);
        }
      }
    }

    stats.push({ tabla: tableName, respaldo: srcRows.length, nuevos: inserted, actualizados: updated, omitidosYaExistian: skipped, errores: errors });
  }

  // --- 1. sec_secretarias ---
  await syncTable(
    'sec_secretarias',
    (row, tgtRows) => {
      const found = tgtRows.find((t) => t.id === row.id || (row.codigo && normText(t.codigo) === normText(row.codigo)));
      if (found) secIdMap.set(row.id, found.id);
      else secIdMap.set(row.id, row.id);
      return found;
    },
    (row) => row
  );

  // --- 2. profiles ---
  await syncTable(
    'profiles',
    (row, tgtRows) => {
      const doc = cleanDoc(row.documento_identidad);
      const email = normText(row.email);
      const found = tgtRows.find(
        (t) =>
          t.id === row.id ||
          (doc && cleanDoc(t.documento_identidad) === doc) ||
          (email && normText(t.email) === email)
      );
      if (found) profileIdMap.set(row.id, found.id);
      else profileIdMap.set(row.id, row.id);
      return found;
    },
    (row) => ({
      ...row,
      secretaria_id: row.secretaria_id ? secIdMap.get(row.secretaria_id) || row.secretaria_id : null,
    })
  );

  // --- 3. contratos ---
  await syncTable(
    'contratos',
    (row, tgtRows) => {
      const found = tgtRows.find(
        (t) =>
          t.id === row.id ||
          (t.contratista_id === row.contratista_id &&
            normText(t.contrato_nro) === normText(row.contrato_nro))
      );
      if (found) contratoIdMap.set(row.id, found.id);
      else contratoIdMap.set(row.id, row.id);
      return found;
    },
    (row) => ({
      ...row,
      contratista_id: row.contratista_id ? profileIdMap.get(row.contratista_id) || row.contratista_id : null,
      secretaria_id: row.secretaria_id ? secIdMap.get(row.secretaria_id) || row.secretaria_id : null,
    })
  );

  // --- 4. informes_mensuales ---
  await syncTable(
    'informes_mensuales',
    (row, tgtRows) => {
      const found = tgtRows.find(
        (t) =>
          t.id === row.id ||
          (t.contrato_id === row.contrato_id &&
            Number(t.informe_nro) === Number(row.informe_nro) &&
            normText(t.tipo_informe || 'Mensual') === normText(row.tipo_informe || 'Mensual'))
      );
      if (found) informeIdMap.set(row.id, found.id);
      else informeIdMap.set(row.id, row.id);
      return found;
    },
    (row) => ({
      ...row,
      contrato_id: row.contrato_id ? contratoIdMap.get(row.contrato_id) || row.contrato_id : null,
    })
  );

  // Conjuntos de informes que ya tenían obligaciones/anexos en la BD original para no duplicar hijos
  const targetInformesWithObligaciones = new Set(targetData.informe_obligaciones.map((o) => o.informe_id));
  const targetInformesWithAnexos = new Set(targetData.informe_anexos.map((a) => a.informe_id));

  // --- 5. informe_obligaciones ---
  await syncTable(
    'informe_obligaciones',
    (row, tgtRows) =>
      tgtRows.find(
        (t) =>
          t.id === row.id ||
          (t.informe_id === row.informe_id && Number(t.orden) === Number(row.orden))
      ),
    (row) => {
      const mappedInfId = row.informe_id ? informeIdMap.get(row.informe_id) || row.informe_id : null;
      if (mappedInfId && existingTargetInformeIds.has(mappedInfId) && targetInformesWithObligaciones.has(mappedInfId)) {
        return null;
      }
      return { ...row, informe_id: mappedInfId };
    }
  );

  // --- 6. informe_anexos ---
  await syncTable(
    'informe_anexos',
    (row, tgtRows) =>
      tgtRows.find(
        (t) =>
          t.id === row.id ||
          (t.informe_id === row.informe_id &&
            ((t.imagen_url && row.imagen_url && t.imagen_url === row.imagen_url) ||
              Number(t.orden) === Number(row.orden)))
      ),
    (row) => {
      const mappedInfId = row.informe_id ? informeIdMap.get(row.informe_id) || row.informe_id : null;
      if (mappedInfId && existingTargetInformeIds.has(mappedInfId) && targetInformesWithAnexos.has(mappedInfId)) {
        return null;
      }
      return { ...row, informe_id: mappedInfId };
    }
  );

  // --- 7. certificaciones_supervision ---
  await syncTable(
    'certificaciones_supervision',
    (row, tgtRows) => {
      const doc = cleanDoc(row.contratista_documento);
      return tgtRows.find(
        (t) =>
          t.id === row.id ||
          (row.informe_id && t.informe_id === row.informe_id) ||
          (doc && cleanDoc(t.contratista_documento) === doc && String(t.pago_nro || '') === String(row.pago_nro || ''))
      );
    },
    (row) => ({
      ...row,
      informe_id: row.informe_id ? informeIdMap.get(row.informe_id) || row.informe_id : null,
      contrato_id: row.contrato_id ? contratoIdMap.get(row.contrato_id) || row.contrato_id : null,
      supervisor_id: row.supervisor_id ? profileIdMap.get(row.supervisor_id) || row.supervisor_id : null,
    })
  );

  // --- 8. soportes_fiduciaria ---
  await syncTable(
    'soportes_fiduciaria',
    (row, tgtRows) => {
      const doc = cleanDoc(row.contratista_documento);
      return tgtRows.find(
        (t) =>
          t.id === row.id ||
          (row.informe_id && t.informe_id === row.informe_id) ||
          (doc && cleanDoc(t.contratista_documento) === doc && String(t.pago_nro || '') === String(row.pago_nro || ''))
      );
    },
    (row) => ({
      ...row,
      informe_id: row.informe_id ? informeIdMap.get(row.informe_id) || row.informe_id : null,
      contrato_id: row.contrato_id ? contratoIdMap.get(row.contrato_id) || row.contrato_id : null,
    })
  );

  // --- 9. declaraciones_renta ---
  await syncTable(
    'declaraciones_renta',
    (row, tgtRows) => {
      const doc = cleanDoc(row.contratista_documento);
      return tgtRows.find(
        (t) =>
          t.id === row.id ||
          (row.informe_id && t.informe_id === row.informe_id) ||
          (doc && cleanDoc(t.contratista_documento) === doc && String(t.pago_nro || '') === String(row.pago_nro || ''))
      );
    },
    (row) => ({
      ...row,
      informe_id: row.informe_id ? informeIdMap.get(row.informe_id) || row.informe_id : null,
      contrato_id: row.contrato_id ? contratoIdMap.get(row.contrato_id) || row.contrato_id : null,
    })
  );

  // --- 10. autorizaciones_desembolso ---
  await syncTable(
    'autorizaciones_desembolso',
    (row, tgtRows) => {
      const doc = cleanDoc(row.contratista_documento);
      return tgtRows.find(
        (t) =>
          t.id === row.id ||
          (row.informe_id && t.informe_id === row.informe_id) ||
          (doc && cleanDoc(t.contratista_documento) === doc && String(t.pago_nro || '') === String(row.pago_nro || ''))
      );
    },
    (row) => ({
      ...row,
      informe_id: row.informe_id ? informeIdMap.get(row.informe_id) || row.informe_id : null,
    })
  );

  // --- 11. notificaciones ---
  await syncTable(
    'notificaciones',
    (row, tgtRows) =>
      tgtRows.find(
        (t) =>
          t.id === row.id ||
          (normText(t.user_id) === normText(row.user_id) &&
            normText(t.tipo) === normText(row.tipo) &&
            String(t.informe_nro || '') === String(row.informe_nro || '') &&
            normText(t.mensaje) === normText(row.mensaje))
      ),
    (row) => ({
      ...row,
      report_id: row.report_id ? informeIdMap.get(row.report_id) || row.report_id : null,
    })
  );

  // --- 12. configuracion_ia ---
  await syncTable(
    'configuracion_ia',
    (row, tgtRows) => tgtRows.find((t) => t.id === row.id),
    (row) => row
  );

  // --- 13. informes_finales ---
  await syncTable(
    'informes_finales',
    (row, tgtRows) => {
      const doc = cleanDoc(row.documento_identidad);
      return tgtRows.find((t) => t.id === row.id || (doc && cleanDoc(t.documento_identidad) === doc));
    },
    (row) => ({
      ...row,
      user_id: row.user_id ? profileIdMap.get(row.user_id) || row.user_id : null,
    })
  );

  console.log('📊 Resumen de tablas:');
  console.table(stats);

  if (!SKIP_STORAGE) {
    console.log('\n🗂️  Revisando archivos de Supabase Storage (anexos y firma)...');
    const storageStats = await syncStorageBuckets();
    console.table(storageStats);
  }

  if (!APPLY) {
    console.log('\nℹ️  Esto fue una SIMULACIÓN. Si los números se ven bien, ejecuta con --apply para guardar en la BD original.');
  } else {
    console.log('\n✅ Sincronización completada.');
  }
}

run().catch((err) => {
  console.error('❌ Error fatal en la sincronización:', err);
  process.exit(1);
});
