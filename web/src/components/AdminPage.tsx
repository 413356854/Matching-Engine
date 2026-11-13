import { useState } from 'react';
import type { Currency, TradingPair } from '../api';
import { FundsFlowAdmin } from './FundsFlowAdmin';
import { KlineAdmin } from './KlineAdmin';
import { OrderHistoryAdmin } from './OrderHistoryAdmin';
import { CandleIcon, FlowIcon, OrderIcon } from './icons';
import type { ToastKind } from './ToastStack';

export type AdminSection = 'flows' | 'orders' | 'klines';

interface AdminPageProps {
  pairs: TradingPair[];
  currencies: Currency[];
  currencyScales: Record<string, number>;
  online: boolean | null;
  notify: (kind: ToastKind, text: string) => void;
}

const SECTIONS = [
  { id: 'flows' as const, label: '流水管理', hint: 'funds_flow', Icon: FlowIcon },
  { id: 'orders' as const, label: '委托历史', hint: 'ord_history', Icon: OrderIcon },
  { id: 'klines' as const, label: 'K 线管理', hint: 'md_kline', Icon: CandleIcon },
];

export function AdminPage({
  pairs,
  currencies,
  currencyScales,
  online,
  notify,
}: AdminPageProps) {
  const [section, setSection] = useState<AdminSection>('flows');

  return (
    <div className="admin-shell">
      <aside className="admin-sidebar panel">
        <div className="admin-sidebar-head">
          <h2>后台管理</h2>
          <span>QuestDB 查询投影</span>
        </div>

        <nav className="admin-nav" aria-label="后台管理子栏目">
          {SECTIONS.map(({ id, label, hint, Icon }) => (
            <button
              key={id}
              type="button"
              className={`admin-nav-item ${section === id ? 'active' : ''}`}
              aria-current={section === id ? 'page' : undefined}
              onClick={() => setSection(id)}
            >
              <Icon />
              <span>{label}</span>
              <em>{hint}</em>
            </button>
          ))}
        </nav>

        <div className="admin-sidebar-foot">
          <i className={online === false ? 'offline' : online === null ? 'pending' : 'online'} />
          {online === null ? 'REST API 连接中' : online ? 'REST API 已连接' : 'REST API 未连接'}
        </div>
      </aside>

      <main className="admin-content">
        {section === 'flows' ? (
          <FundsFlowAdmin
            currencies={currencies}
            currencyScales={currencyScales}
            notify={notify}
          />
        ) : section === 'orders' ? (
          <OrderHistoryAdmin />
        ) : (
          <KlineAdmin pairs={pairs} currencyScales={currencyScales} notify={notify} />
        )}
      </main>
    </div>
  );
}
