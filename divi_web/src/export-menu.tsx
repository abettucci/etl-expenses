import { useState } from 'react'
import * as XLSX from 'xlsx'
import { jsPDF } from 'jspdf'
import autoTable from 'jspdf-autotable'
import './phase1.css'

export type ExportColumn = { key: string; label: string }
type Row = Record<string, unknown>
type Props = { title: string; filename: string; columns: ExportColumn[]; rows?: Row[]; load?: () => Promise<Row[]>; disabled?: boolean }

function plain(value: unknown): string | number | boolean {
  if (value === null || value === undefined) return ''
  if (typeof value === 'number' || typeof value === 'boolean') return value
  return String(value)
}

function matrix(columns: ExportColumn[], rows: Record<string, unknown>[]) {
  return rows.map(row => Object.fromEntries(columns.map(c => [c.label, plain(row[c.key])])))
}

function download(blob: Blob, name: string) {
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = name
  document.body.appendChild(link)
  link.click()
  link.remove()
  setTimeout(() => URL.revokeObjectURL(url), 1000)
}

// Quotes every cell, doubles embedded quotes, and prefixes formulas so that
// spreadsheet apps never run a value copied from a merchant name.
function csvCell(value: unknown): string {
  const text = String(plain(value))
  const safe = /^[=+\-@\t\r]/.test(text) ? `'${text}` : text
  return `"${safe.replace(/"/g, '""')}"`
}

export function exportCsv(columns: ExportColumn[], rows: Record<string, unknown>[], filename: string) {
  const header = columns.map(c => csvCell(c.label)).join(',')
  const body = rows.map(row => columns.map(c => csvCell(row[c.key])).join(','))
  download(new Blob(['﻿' + [header, ...body].join('\r\n')], { type: 'text/csv;charset=utf-8' }), `${filename}.csv`)
}

export function exportXlsx(columns: ExportColumn[], rows: Record<string, unknown>[], filename: string, title: string) {
  const sheet = XLSX.utils.json_to_sheet(matrix(columns, rows))
  const book = XLSX.utils.book_new()
  XLSX.utils.book_append_sheet(book, sheet, title.slice(0, 31) || 'Datos')
  XLSX.writeFile(book, `${filename}.xlsx`)
}

export function exportPdf(columns: ExportColumn[], rows: Record<string, unknown>[], filename: string, title: string) {
  const doc = new jsPDF({ orientation: 'landscape', unit: 'pt', format: 'a4' })
  const generated = new Date().toLocaleString('es-AR')
  doc.setFontSize(14)
  doc.text(title, 40, 40)
  doc.setFontSize(9)
  doc.text(`${rows.length.toLocaleString('es-AR')} filas · generado ${generated}`, 40, 56)
  autoTable(doc, {
    startY: 70,
    head: [columns.map(c => c.label)],
    body: rows.map(row => columns.map(c => String(plain(row[c.key])))),
    styles: { fontSize: 7.5, cellPadding: 3 },
    headStyles: { fillColor: [23, 76, 54] },
    margin: { left: 40, right: 40 },
  })
  doc.save(`${filename}.pdf`)
}

export function ExportMenu({ title, filename, columns, rows = [], load, disabled }: Props) {
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState('')
  const [empty, setEmpty] = useState(rows.length === 0)
  async function run(format: 'csv' | 'xlsx' | 'pdf') {
    setMessage(''); setBusy(true)
    try {
      const data = load ? await load() : rows
      setEmpty(data.length === 0)
      if (data.length === 0) { setMessage('Sin filas para exportar.'); return }
      if (format === 'csv') exportCsv(columns, data, filename)
      if (format === 'xlsx') exportXlsx(columns, data, filename, title)
      if (format === 'pdf') exportPdf(columns, data, filename, title)
    } catch {
      setMessage('No pudimos generar el archivo. Intentá de nuevo.')
    } finally {
      setBusy(false)
    }
  }
  return <div className="export-menu" role="group" aria-label={`Descargar ${title}`}>
    <span className="export-label">Descargar</span>
    {(['csv', 'xlsx', 'pdf'] as const).map(format => <button key={format} className="outline export-btn" type="button"
      disabled={disabled || busy || (!load && empty)} onClick={() => void run(format)}>{format.toUpperCase()}</button>)}
    {message && <small className="export-note" role="alert">{message}</small>}
  </div>
}
