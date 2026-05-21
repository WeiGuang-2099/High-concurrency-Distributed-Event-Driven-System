# PRD: P9 - Wrap-up (README + Architecture Diagrams + Interview Prep)

## Introduction

项目收尾：编写高质量 README、生成架构图、准备面试要点文档。这个阶段决定面试官打开你 GitHub 时的第一印象。一份清晰的 README 和可视化架构图比代码本身更能传达你的设计能力。

## Goals

- README.md 是完整的项目说明：30 秒内让面试官理解项目在做什么、用了什么技术、解决了什么问题
- 架构图直观展示系统设计，面试时可以作为视觉辅助
- 面试准备文档覆盖所有核心技术点的 Q&A
- GitHub 仓库呈现专业、完整的项目形象

## User Stories

### US-001: Write project README.md
**Description:** As a job seeker, I need a professional README so interviewers quickly understand the project's value.

**Acceptance Criteria:**
- [ ] Project title, one-line description, and badge row (Java 17, Spring Boot 3, Kafka, etc.)
- [ ] Architecture overview: ASCII or embedded image of system architecture diagram
- [ ] Tech stack table with all technologies and their purpose
- [ ] Core highlights section: 4 technical highlights with 2-3 sentence explanation each
- [ ] Quick start section: `git clone` -> `docker-compose up` -> access URLs
- [ ] API documentation link (Swagger UI URL)
- [ ] Load test results section: embed K6 summary screenshots with key metrics
- [ ] Project structure overview with brief description of each service
- [ ] Not another "TODO app" section explaining why this project demonstrates senior-level thinking

### US-002: Generate system architecture diagram
**Description:** As a job seeker, I need a clear architecture diagram for README and interview use.

**Acceptance Criteria:**
- [ ] High-level architecture diagram showing all 7 services, infrastructure, and data flow
- [ ] Data flow diagram for core auction bid process (step by step)
- [ ] Data flow diagram for ticket oversell prevention (step by step)
- [ ] Distributed transaction sequence diagram (Seata 2PC flow)
- [ ] All diagrams saved as PNG in `docs/architecture/`
- [ ] Diagrams use consistent color coding: services (blue), infrastructure (green), data flow (arrows)

### US-003: Write interview preparation document
**Description:** As a job seeker, I need a document that prepares me for technical questions about this project.

**Acceptance Criteria:**
- [ ] `docs/interview-prep/technical-qa.md` covering 20+ Q&A pairs
- [ ] Q&A organized by topic: system design, concurrency, distributed TX, event-driven, observability
- [ ] Each answer: 2-3 minute oral response length, includes "why this approach" reasoning
- [ ] Sample questions:
  - "How do you prevent overselling under 10K concurrent requests?"
  - "Why Kafka AND RabbitMQ instead of just one?"
  - "Walk me through what happens when a user places a bid"
  - "How does your event sourcing handle failures?"
  - "What happens if Redis goes down?"
  - "How do you ensure exactly-once processing in Kafka?"
  - "Why Seata AT mode instead of TCC or Saga?"
  - "How would you scale this system to 100K users?"

### US-004: Write deployment and demo guide
**Description:** As a job seeker, I need a guide for running the demo in an interview setting.

**Acceptance Criteria:**
- [ ] `docs/demo-guide.md` with step-by-step demo script
- [ ] Prerequisites: Docker, memory requirements, ports used
- [ ] Demo flow: register 2 users -> create auction -> both bid in real-time -> show settlement
- [ ] Demo flow: create 10 tickets -> show oversell prevention (concurrent purchase)
- [ ] Demo flow: show Kibana dashboard with live logs
- [ ] Demo flow: show Zipkin trace for a single request
- [ ] Troubleshooting section: common issues and fixes

### US-005: Clean up code and add Swagger/OpenAPI
**Description:** As a job seeker, I need clean, well-documented code so the project looks professional on GitHub.

**Acceptance Criteria:**
- [ ] Swagger/OpenAPI UI accessible at gateway `http://localhost:8080/swagger-ui.html`
- [ ] Each API endpoint has description and example request/response
- [ ] No `System.out.println` or debug logging in committed code
- [ ] No hardcoded values (all via Nacos config)
- [ ] Consistent code style across all services

### US-006: Create .gitignore and repository setup
**Description:** As a job seeker, I need a clean Git repository with no build artifacts or sensitive files.

**Acceptance Criteria:**
- [ ] `.gitignore` excludes: target/, node_modules/, .idea/, *.iml, .env, logs/, Docker volumes
- [ ] No secrets in committed files (all passwords use placeholder defaults for demo)
- [ ] Git initialized with clean initial commit
- [ ] Branch strategy: `main` for stable, feature branches per phase

## Functional Requirements

- FR-1: README.md covers all sections: overview, architecture, tech stack, highlights, quick start, results
- FR-2: Architecture diagrams (4 types) saved as PNG files
- FR-3: Interview prep document with 20+ Q&A pairs
- FR-4: Demo guide with scripted demo flow
- FR-5: Swagger UI available at gateway with all API documentation
- FR-6: Clean .gitignore, no committed secrets or build artifacts

## Non-Goals

- No GitHub Actions CI/CD pipeline
- No custom domain or deployed URL (local Docker only)
- No video demo recording
- No portfolio website integration
- No blog post about the project (can be added later)

## Technical Considerations

- Architecture diagrams: use draw.io, Excalidraw, or Mermaid (Mermaid renders in GitHub README natively)
- Swagger/OpenAPI: Springdoc OpenAPI (SpringFox is abandoned) with Spring Boot 3
- Swagger aggregation approach: each microservice exposes `/v3/api-docs` endpoint via springdoc-openapi; gateway-service aggregates all service specs using `SpringDocProvider` with service discovery (Nacos); single Swagger UI at `http://localhost:8080/swagger-ui.html` shows all service APIs grouped by service name
- Gateway Swagger config: add `springdoc.swagger-ui.urls` in Nacos config listing each service's `/v3/api-docs` path via Nacos service instances
- README badges from shields.io
- K6 report screenshots: run tests, capture Kibana/K6 output, save to `docs/screenshots/`

## Success Metrics

- Interviewer can understand the project in < 30 seconds of reading README
- Architecture diagrams render correctly in GitHub markdown
- Demo guide allows someone unfamiliar with the project to run it in < 10 minutes
- Interview prep covers all likely technical questions about the project
