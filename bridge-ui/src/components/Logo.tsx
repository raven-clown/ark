export function Wordmark({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 492 196" role="img" aria-label="ARK">
      <g fill="none" stroke="#E8EDF2" strokeWidth="26" strokeLinecap="round" strokeLinejoin="round">
        <g transform="translate(-41.5 -27.96) scale(0.49)">
          <path d="M146 404 C146 262 196 164 256 110 C316 164 366 262 366 404" strokeWidth="53" />
        </g>
        <path d="M212 170 V30 H270 C318 30 318 104 270 104 H212 M272 104 L318 170" />
        <path d="M380 30 V170 M456 30 L382 116 M416 76 L462 170" />
      </g>
      <circle cx="83.9" cy="122.9" r="16" fill="#2EE6A6" opacity="0.18" />
      <circle cx="83.9" cy="122.9" r="9" fill="#2EE6A6" />
    </svg>
  )
}

export function Mark({ className, flow = '#2EE6A6' }: { className?: string; flow?: string }) {
  return (
    <svg className={className} viewBox="100 80 312 350" aria-hidden="true">
      <path d="M146 404 C146 262 196 164 256 110 C316 164 366 262 366 404" fill="none" stroke="#E8EDF2" strokeWidth="46" strokeLinecap="round" strokeLinejoin="round" />
      <circle cx="256" cy="308" r="31" fill={flow} opacity="0.18" />
      <circle cx="256" cy="308" r="17" fill={flow} />
    </svg>
  )
}
