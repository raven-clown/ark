import { useEffect, useRef } from 'react'
import { LineChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import * as echarts from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'

echarts.use([LineChart, GridComponent, TooltipComponent, LegendComponent, CanvasRenderer])

// Series colors validated for the dark console surface (#101318): lightness
// band, chroma, CVD and normal-vision separation, 3:1 contrast. Rejected and
// DLQ sit in the CVD warning band, so each series also has its own dash.
export const SERIES = {
  processed: { color: '#1AA578', dash: 'solid' as const },
  rejected: { color: '#B08A1F', dash: 'dashed' as const },
  dlq: { color: '#E2466B', dash: 'dotted' as const },
  single: { color: '#1AA578', dash: 'solid' as const },
}

const SURFACE = '#101318'
const INK = '#E8EDF2'
const INK_2 = '#8B96A3'
const INK_3 = '#5E6873'
const GRID = '#1C2229'

export interface Series {
  name: string
  kind: keyof typeof SERIES
  points: [string, number][]
}

function hexA(hex: string, a: number) {
  const n = parseInt(hex.slice(1), 16)
  return `rgba(${n >> 16}, ${(n >> 8) & 255}, ${n & 255}, ${a})`
}

function compact(v: number) {
  if (Math.abs(v) >= 1e6) return (v / 1e6).toFixed(1) + 'M'
  if (Math.abs(v) >= 1e3) return (v / 1e3).toFixed(1) + 'K'
  if (Math.abs(v) >= 100 || v === 0) return Math.round(v).toLocaleString()
  return v.toFixed(v < 1 ? 2 : 1)
}

export function TimeChart({ series, unit, height = 220, motion = true }: { series: Series[]; unit: string; height?: number; motion?: boolean }) {
  const el = useRef<HTMLDivElement>(null)
  const chart = useRef<echarts.ECharts | null>(null)

  useEffect(() => {
    if (!el.current) return
    const c = echarts.init(el.current, undefined, { renderer: 'canvas' })
    chart.current = c
    const ro = new ResizeObserver(() => c.resize())
    ro.observe(el.current)
    return () => {
      ro.disconnect()
      c.dispose()
      chart.current = null
    }
  }, [])

  useEffect(() => {
    const multi = series.length > 1
    const pts = series[0]?.points ?? []
    const shortSpan = pts.length < 2 || Date.parse(pts[pts.length - 1][0]) - Date.parse(pts[0][0]) < 10 * 60 * 1000
    chart.current?.setOption(
      {
        animation: motion,
        animationDuration: 700,
        animationDurationUpdate: 600,
        animationEasingUpdate: 'cubicOut',
        textStyle: { fontFamily: 'Inter, ui-sans-serif, system-ui, sans-serif', color: INK_2 },
        grid: { left: 8, right: multi ? 16 : 56, top: multi ? 40 : 16, bottom: 8, containLabel: true },
        legend: multi
          ? { top: 0, left: 0, icon: 'roundRect', itemWidth: 14, itemHeight: 4, itemGap: 18, textStyle: { color: INK_2, fontSize: 12 } }
          : { show: false },
        tooltip: {
          trigger: 'axis',
          backgroundColor: 'rgba(21, 26, 32, 0.96)',
          borderColor: '#2A323C',
          borderWidth: 1,
          borderRadius: 10,
          padding: [8, 12],
          textStyle: { color: INK, fontSize: 12 },
          axisPointer: { type: 'line', lineStyle: { color: '#3A444F', width: 1 } },
          valueFormatter: (v: unknown) => `${compact(Number(v))} ${unit}`,
        },
        xAxis: {
          type: 'time',
          boundaryGap: false,
          axisLine: { lineStyle: { color: GRID } },
          axisTick: { show: false },
          axisLabel: { color: INK_3, fontSize: 11, hideOverlap: true, formatter: shortSpan ? '{HH}:{mm}:{ss}' : '{HH}:{mm}' },
          splitLine: { show: false },
        },
        yAxis: {
          type: 'value',
          min: 0,
          splitNumber: 4,
          axisLabel: { color: INK_3, fontSize: 11, formatter: (v: number) => compact(v) },
          splitLine: { lineStyle: { color: GRID, width: 1, type: 'solid' } },
        },
        series: series.map((s) => {
          const spec = SERIES[s.kind]
          return {
            name: s.name,
            type: 'line',
            smooth: 0.35,
            showSymbol: false,
            symbol: 'circle',
            symbolSize: 8,
            data: s.points,
            lineStyle: { width: 2, color: spec.color, type: spec.dash, cap: 'round', join: 'round' },
            itemStyle: { color: spec.color, borderColor: SURFACE, borderWidth: 2 },
            emphasis: { focus: 'series', scale: 1.25 },
            areaStyle: {
              color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                { offset: 0, color: hexA(spec.color, multi ? 0.12 : 0.2) },
                { offset: 1, color: hexA(spec.color, 0) },
              ]),
            },
            endLabel: multi ? undefined : { show: true, color: INK, fontSize: 12, fontWeight: 600, formatter: (p: { value: [string, number] }) => compact(p.value[1]) },
          }
        }),
      },
      { replaceMerge: ['series'] },
    )
  }, [series, unit, motion])

  return <div ref={el} style={{ width: '100%', height }} />
}
