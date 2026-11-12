import { LogoMark } from './icons';

export type AppView = 'trade' | 'admin';

interface HeaderProps {
  userId: number;
  online: boolean | null;
  view: AppView;
  onNavigate: (view: AppView) => void;
  onOpenLogin: () => void;
}

export function Header({ userId, online, view, onNavigate, onOpenLogin }: HeaderProps) {
  const statusText = online === null ? '连接中' : online ? 'API 在线' : 'API 离线';

  return (
    <header className="topbar">
      <div className="brand">
        <LogoMark />
        <div className="brand-text">
          <strong>ME Exchange</strong>
          <span>Spot Matching Engine</span>
        </div>
      </div>

      <nav className="topnav" aria-label="主导航">
        <button
          type="button"
          className={`nav-item ${view === 'trade' ? 'active' : ''}`}
          aria-current={view === 'trade' ? 'page' : undefined}
          onClick={() => onNavigate('trade')}
        >
          币币交易
        </button>
        <button
          type="button"
          className={`nav-item ${view === 'admin' ? 'active' : ''}`}
          aria-current={view === 'admin' ? 'page' : undefined}
          onClick={() => onNavigate('admin')}
        >
          后台管理
        </button>
      </nav>

      <div className="topbar-right">
        <span className={`conn-state ${online === null ? 'pending' : online ? 'online' : 'offline'}`}>
          <i aria-hidden="true" />
          {statusText}
        </span>

        <span className="user-chip" title="当前登录用户">
          <span className="user-chip-label">用户</span>
          <b>{userId}</b>
        </span>

        <button type="button" className="login-button" onClick={onOpenLogin}>
          模拟登录
        </button>
      </div>
    </header>
  );
}
