/** Same mark as the embedded favicon (KafkaBatch::Web::FAVICON_SVG). */
export function BrandMark({ size = 32 }: { size?: number }) {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width={size}
      height={size}
      viewBox="0 0 32 32"
      fill="none"
      aria-hidden
      focusable="false"
    >
      <defs>
        <linearGradient id="kb-bg" x1="0" y1="0" x2="32" y2="32" gradientUnits="userSpaceOnUse">
          <stop offset="0" stopColor="#0F766E" />
          <stop offset="1" stopColor="#0369A1" />
        </linearGradient>
      </defs>
      <rect width="32" height="32" rx="7" fill="url(#kb-bg)" />
      <g stroke="#FFFFFF" strokeWidth="1.9" strokeLinecap="round" fill="none">
        <path d="M11 16 Q16.5 16 21 8" opacity="0.72" />
        <path d="M11 16 H21" opacity="0.92" />
        <path d="M11 16 Q16.5 16 21 24" opacity="0.72" />
      </g>
      <circle cx="10" cy="16" r="3.1" fill="#FFFFFF" />
      <circle cx="10" cy="16" r="1.3" fill="#0F766E" />
      <rect x="21" y="6.4" width="4.6" height="3.2" rx="1" fill="#FFFFFF" opacity="0.80" />
      <rect x="21" y="14.4" width="4.6" height="3.2" rx="1" fill="#FFFFFF" />
      <rect x="21" y="22.4" width="4.6" height="3.2" rx="1" fill="#FFFFFF" opacity="0.80" />
    </svg>
  )
}
