# PRD: P6 - Frontend (React + TypeScript)

## Introduction

实现前端界面：竞拍首页、实时出价页面、票务购买、用户中心、管理后台。前端是面试 Demo 的核心展示面，面试官通过点击操作直接感受系统的实时性和高并发能力。所有页面需要 WebSocket 实时更新，无需手动刷新。

## Goals

- 面试官可以完整体验"注册 -> 浏览竞拍 -> 出价 -> 购票 -> 查看订单"全流程
- 竞拍页面实时更新出价、倒计时，WebSocket 推送无感知
- 管理后台可以创建竞拍和票务，查看实时数据
- 响应式布局适配桌面端（不需要移动端）

## User Stories

### US-001: Login & Register pages
**Description:** As a user, I want to register and login so I can participate in auctions.

**Acceptance Criteria:**
- [ ] `/login` page has username + password form, calls `POST /api/users/login`
- [ ] `/register` page has username + email + password form, calls `POST /api/users/register`
- [ ] JWT stored in localStorage after login
- [ ] Axios interceptor attaches JWT to all API requests
- [ ] 401 response triggers redirect to login page
- [ ] Verify in browser

### US-002: Home page with auction list
**Description:** As a user, I want to see hot auctions on the homepage so I can quickly find something to bid on.

**Acceptance Criteria:**
- [ ] `/` shows hot auctions (GET /api/auctions/hot) with cards showing: image placeholder, event name, current price, time remaining, bid count
- [ ] Countdown timer updates every second (client-side countdown)
- [ ] Cards link to auction detail page
- [ ] Loading skeleton while data fetches
- [ ] Verify in browser

### US-003: Auction detail page with real-time bidding
**Description:** As a user, I want to view an auction and place bids in real-time so I can compete with other bidders.

**Acceptance Criteria:**
- [ ] `/auctions/:id` shows: event name, description, current highest bid (large font), countdown timer, bid history list, bid input form
- [ ] WebSocket connects on page mount, subscribes to `/topic/auction/{id}`
- [ ] New bids appear in real-time: current price updates, bid history prepends entry
- [ ] Bid input: amount field with "Bid" button, validates amount > current price client-side
- [ ] Success: shows "Bid placed!" toast notification
- [ ] Failure: shows error toast with reason
- [ ] User can only bid if logged in (button disabled otherwise, shows "Login to bid")
- [ ] Verify in browser

### US-004: Ticket purchase page
**Description:** As a user, I want to buy tickets for an event so I can attend.

**Acceptance Criteria:**
- [ ] `/tickets/:eventId` shows available ticket types with stock count (real-time via WebSocket)
- [ ] Each ticket type card shows: type name, price, available count, quantity selector, "Buy" button
- [ ] Clicking "Buy": calls reserve API, then redirects to order payment page
- [ ] Stock count decrements in real-time when other users purchase
- [ ] "Out of stock" shown with disabled button when count reaches 0
- [ ] Verify in browser

### US-005: Order payment page
**Description:** As a user, I want to pay for my order so my purchase is confirmed.

**Acceptance Criteria:**
- [ ] `/orders/:id/pay` shows order summary: item, amount, status
- [ ] "Pay Now" button triggers mock payment (calls pay API)
- [ ] Loading spinner during payment processing
- [ ] Success: shows confirmation page with green check icon
- [ ] Failure: shows retry button with error message
- [ ] Verify in browser

### US-006: User center - my bids and orders
**Description:** As a user, I want to see my bidding history and orders so I can track my activity.

**Acceptance Criteria:**
- [ ] `/profile` shows two tabs: "My Bids" and "My Orders"
- [ ] "My Bids": paginated list of auctions user bid on, showing status (winning/outbid/settled)
- [ ] "My Orders": paginated list of orders with status badge (CREATED=yellow, PAID=green, CANCELLED=red)
- [ ] Click order row navigates to order detail
- [ ] Verify in browser

### US-007: Admin panel - create auction & tickets
**Description:** As an admin, I want to create auctions and ticket batches so I have content for the demo.

**Acceptance Criteria:**
- [ ] `/admin` has sidebar navigation: Create Auction, Create Tickets, Dashboard
- [ ] Create Auction form: event name, description, starting price, start/end time, linked ticket type
- [ ] Create Tickets form: event ID, ticket type, quantity, price
- [ ] Dashboard: shows active auctions count, total bids today, total orders today, total revenue
- [ ] All admin APIs require admin role (checked via JWT roles claim)
- [ ] Verify in browser

### US-008: Global notification component
**Description:** As a user, I want to receive real-time notifications when I'm outbid or my order status changes.

**Acceptance Criteria:**
- [ ] Bell icon in top navigation shows unread count badge
- [ ] WebSocket listens to user-specific notification channel
- [ ] New notification: toast popup in bottom-right corner + badge increment
- [ ] Click bell: dropdown shows recent notifications
- [ ] Verify in browser

## Functional Requirements

- FR-1: Login/Register pages with JWT token management
- FR-2: Home page displays hot auctions with countdown timers
- FR-3: Auction detail page with WebSocket real-time bid updates and bid placement form
- FR-4: Ticket purchase page with real-time stock count and reservation flow
- FR-5: Order payment page with mock payment integration
- FR-6: User center with bid history and order list tabs
- FR-7: Admin panel with auction/ticket creation forms and statistics dashboard
- FR-8: Global notification system with WebSocket push and toast display
- FR-9: Axios interceptor for JWT attachment and 401 handling
- FR-10: All pages use Ant Design Pro components for consistent UI

## Non-Goals

- No mobile responsive design (desktop only, min-width 1024px)
- No dark mode toggle
- No internationalization (English only)
- No image upload for auctions (placeholder images only)
- No SSR/SSG (SPA with client-side routing)
- No E2E tests for frontend (manual verification in browser)

## Technical Considerations

- React 18 + TypeScript + Vite for fast dev builds
- Vite dev server proxy configuration: proxy `/api/*` to `http://localhost:8080`, proxy `/ws/*` to `http://localhost:8080`
- React Query (TanStack Query) for server state: caching, refetching, optimistic updates
- Zustand for client state: currentUser, WebSocket connection status
- Ant Design Pro as component library (table, form, card, notification, etc.)
- WebSocket via `@stomp/stompjs` + `sockjs-client` with auto-reconnect (5s interval, max 10 retries)
- React Router v6 for routing
- API client: Axios instance with JWT interceptor
- Admin role check: frontend decodes JWT payload to check `roles` claim, shows admin panel link only for ADMIN role
- Error handling: Axios response interceptor catches 4xx/5xx, displays Ant Design `message.error()` toast with error message from unified error response format

## Success Metrics

- Full user flow (register -> browse -> bid -> buy -> check orders) works in < 5 clicks
- WebSocket updates appear within 500ms of event
- Page load time < 2s on local development
- All pages pass visual check in Chrome
