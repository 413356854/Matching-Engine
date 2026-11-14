export function LogoMark() {
  return (
    <svg viewBox="0 0 32 32" width="30" height="30" aria-hidden="true">
      <rect x="1" y="1" width="30" height="30" rx="8" fill="#161b22" stroke="#2a323c" />
      <path d="M9 21.5V12M9 12l6-4v10l-6 3.5Z" fill="#f0b90b" opacity="0.95" />
      <path d="M17.5 24V9.5M17.5 22l6.5-3.5V8l-6.5 3.5" fill="none" stroke="#0ecb81" strokeWidth="1.6" />
      <circle cx="23.5" cy="8" r="1.6" fill="#0ecb81" />
    </svg>
  );
}

export function RefreshIcon({ spinning = false }: { spinning?: boolean }) {
  return (
    <svg
      viewBox="0 0 20 20"
      width="15"
      height="15"
      aria-hidden="true"
      className={spinning ? 'spin' : undefined}
    >
      <path
        d="M16.2 8.2A6.4 6.4 0 1 0 16 12.3M16.2 8.2V3.8m0 4.4h-4.4"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

export function SearchIcon() {
  return (
    <svg viewBox="0 0 20 20" width="14" height="14" aria-hidden="true">
      <circle cx="8.7" cy="8.7" r="5.2" fill="none" stroke="currentColor" strokeWidth="1.5" />
      <path d="m12.7 12.7 4 4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

export function WalletIcon() {
  return (
    <svg viewBox="0 0 20 20" width="16" height="16" aria-hidden="true">
      <rect x="2.5" y="5" width="15" height="11" rx="2.5" fill="none" stroke="currentColor" strokeWidth="1.4" />
      <path d="M2.5 8h15" stroke="currentColor" strokeWidth="1.4" />
      <circle cx="14.5" cy="12" r="1.2" fill="currentColor" />
    </svg>
  );
}

export function BoltIcon() {
  return (
    <svg viewBox="0 0 20 20" width="14" height="14" aria-hidden="true">
      <path d="M11.2 2.5 4.8 11h4.1l-.9 6.5L15.2 9h-4.1l.1-6.5Z" fill="currentColor" />
    </svg>
  );
}

export function ChevronDownIcon() {
  return (
    <svg viewBox="0 0 16 16" width="12" height="12" aria-hidden="true">
      <path
        d="m3.5 6 4.5 4.5L12.5 6"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

export function FlowIcon() {
  return (
    <svg viewBox="0 0 20 20" width="16" height="16" aria-hidden="true">
      <path d="M3.5 5h13M3.5 10h13M3.5 15h13" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
      <circle cx="7" cy="5" r="1.5" fill="currentColor" />
      <circle cx="12.5" cy="10" r="1.5" fill="currentColor" />
      <circle cx="9" cy="15" r="1.5" fill="currentColor" />
    </svg>
  );
}

export function OrderIcon() {
  return (
    <svg viewBox="0 0 20 20" width="16" height="16" aria-hidden="true">
      <rect x="4" y="2.5" width="12" height="15" rx="2" fill="none" stroke="currentColor" strokeWidth="1.3" />
      <path d="M7 6.5h6M7 10h6M7 13.5h3.5" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" />
    </svg>
  );
}

export function TradeIcon() {
  return (
    <svg viewBox="0 0 20 20" width="16" height="16" aria-hidden="true">
      <path
        d="M3 7h11M11.5 4.5 14 7l-2.5 2.5M17 13H6M8.5 10.5 6 13l2.5 2.5"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.4"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

export function CandleIcon() {
  return (
    <svg viewBox="0 0 20 20" width="16" height="16" aria-hidden="true">
      <path d="M5.5 3v3.2M5.5 13.8V17M14.5 2.5v2M14.5 12v5.5" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" />
      <rect x="3.5" y="6.2" width="4" height="7.6" rx="1" fill="none" stroke="currentColor" strokeWidth="1.3" />
      <rect x="12.5" y="4.5" width="4" height="7.5" rx="1" fill="currentColor" opacity="0.3" stroke="currentColor" strokeWidth="1.3" />
    </svg>
  );
}
