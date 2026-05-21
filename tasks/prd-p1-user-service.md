# PRD: P1 - User Service & Authentication

## Introduction

实现用户服务和 JWT 认证体系：用户注册、登录、Token 签发、Gateway JWT 验证。这是所有业务服务的前置依赖 -- 没有 auth，后续的竞拍、购票、订单都无法关联到具体用户。

## Goals

- 用户可以注册账号并登录获取 JWT
- Gateway 对所有业务 API 进行 JWT 验证（公开接口除外）
- JWT 使用 RS256 非对称签名，user-service 持私钥签发，gateway 持公钥验证
- 登出时 JWT 加入 Redis 黑名单

## User Stories

### US-001: User registration endpoint
**Description:** As a new user, I want to register with username, email, and password so I can participate in auctions.

**Acceptance Criteria:**
- [ ] `POST /api/users/register` accepts `{username, email, password}`
- [ ] Password stored as BCrypt hash (never plaintext)
- [ ] Username and email must be unique; returns 409 on conflict
- [ ] Returns created user DTO (id, username, email) with 201 status
- [ ] Input validation: username 3-50 chars, email format, password min 8 chars

### US-002: User login and JWT issuance
**Description:** As a registered user, I want to login and receive a JWT so I can make authenticated API calls.

**Acceptance Criteria:**
- [ ] `POST /api/users/login` accepts `{username, password}`
- [ ] Validates credentials against BCrypt hash
- [ ] Returns JWT (RS256 signed) containing: userId, username, roles, jti (unique ID), exp (24h TTL)
- [ ] Returns 401 on invalid credentials
- [ ] JWT signed with RSA private key loaded from keystore file

### US-003: JWT blacklist on logout
**Description:** As a logged-in user, I want to logout so my token becomes invalid immediately.

**Acceptance Criteria:**
- [ ] `POST /api/users/logout` requires valid JWT in Authorization header
- [ ] Stores `token:blacklist:{jti}` in Redis with TTL = remaining token expiry time
- [ ] Returns 200 on success
- [ ] Blacklisted tokens are rejected by Gateway

### US-004: Get and update user profile
**Description:** As a user, I want to view and update my profile information.

**Acceptance Criteria:**
- [ ] `GET /api/users/me` returns current user's profile (from JWT userId)
- [ ] `PUT /api/users/me` accepts updatable fields: email
- [ ] Neither endpoint exposes password hash

### US-005: Gateway JWT authentication filter
**Description:** As the system, I need the gateway to verify JWT on every request so that unauthorized access is blocked before reaching microservices.

**Acceptance Criteria:**
- [ ] Global filter extracts JWT from `Authorization: Bearer <token>` header
- [ ] Verifies JWT signature using RSA public key (no call to user-service needed)
- [ ] Checks `token:blacklist:{jti}` in Redis; rejects if blacklisted
- [ ] Adds `X-User-Id` and `X-Username` headers to forwarded request
- [ ] Public endpoints excluded from auth: `/api/users/register`, `/api/users/login`
- [ ] Returns 401 with clear error message on invalid/missing/expired token

### US-006: RSA key pair setup
**Description:** As a developer, I need RSA key pair configuration so JWT signing and verification work across services.

**Acceptance Criteria:**
- [ ] RSA key pair generated and stored as keystore file in `user-service/src/main/resources/`
- [ ] Public key exported and configured in gateway-service (via Nacos shared config)
- [ ] Key pair can be rotated by updating keystore + Nacos config (no code change)

### US-007: Admin user seed data
**Description:** As a developer, I need a pre-created admin account so I can test admin-only features.

**Acceptance Criteria:**
- [ ] MySQL init script inserts admin user: username=admin, password=BCrypt hash of "admin123", role=ADMIN
- [ ] JWT payload includes `roles` claim with value ["ADMIN"] or ["USER"]
- [ ] Gateway passes `X-User-Roles` header to downstream services alongside `X-User-Id` and `X-Username`

## Functional Requirements

- FR-1: `POST /api/users/register` creates user with BCrypt-hashed password
- FR-2: `POST /api/users/login` validates credentials and returns RS256 JWT
- FR-3: `POST /api/users/logout` blacklists JWT in Redis with correct TTL
- FR-4: `GET /api/users/me` returns profile of authenticated user
- FR-5: `PUT /api/users/me` updates email of authenticated user
- FR-6: Gateway global filter validates JWT on all requests except register/login
- FR-7: Gateway checks Redis blacklist before accepting JWT
- FR-8: Gateway injects user identity headers (`X-User-Id`, `X-Username`) for downstream services
- FR-9: JWT `roles` field supports USER and ADMIN values
- FR-10: Gateway injects `X-User-Roles` header for downstream role checking

## Non-Goals

- No OAuth2/social login (Google, GitHub, etc.)
- No email verification flow
- No password reset functionality
- No fine-grained RBAC (only USER and ADMIN roles)
- No permission management UI

## Technical Considerations

- RS256 (asymmetric) avoids gateway needing to call user-service for every request
- Redis blacklist uses `SET key value EX <seconds>` with TTL matching token remaining life
- BCrypt cost factor of 10 is sufficient for demo (production would use 12+)
- Keystore file format: PKCS12, loaded via Spring Security's `JwtDecoder` / `JwtEncoder`

## Success Metrics

- Register -> Login -> Access protected endpoint -> Logout -> Access fails: full cycle works
- Gateway rejects requests with expired, invalid, or blacklisted tokens
- Authentication adds < 5ms latency per request (Redis blacklist check + JWT decode)
