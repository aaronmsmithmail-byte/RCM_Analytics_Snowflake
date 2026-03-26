// ============================================================
// Healthcare RCM Analytics — Knowledge Graph Seed Data
// ============================================================
// 10 entity nodes + 9 foreign-key relationships representing
// the Silver-layer data model.
//
// Run on first startup via Neo4j's /docker-entrypoint-initdb.d
// or manually via: cypher-shell -f seed.cypher
// ============================================================

// ── Constraints ─────────────────────────────────────────────
CREATE CONSTRAINT entity_id_unique IF NOT EXISTS
FOR (e:Entity) REQUIRE e.entity_id IS UNIQUE;

// ── Reference Entities ──────────────────────────────────────
CREATE (p:Entity:Reference {
  entity_id:     'payers',
  entity_name:   'payers',
  entity_group:  'Reference',
  silver_table:  'silver_payers',
  description:   'payer_id PK, payer_name, payer_type, avg_reimbursement_pct REAL, contract_id',
  source_system: 'Payer Master'
});

CREATE (pt:Entity:Reference {
  entity_id:     'patients',
  entity_name:   'patients',
  entity_group:  'Reference',
  silver_table:  'silver_patients',
  description:   'patient_id PK, first_name, last_name, date_of_birth, gender, primary_payer_id FK → silver_payers',
  source_system: 'EHR'
});

CREATE (pr:Entity:Reference {
  entity_id:     'providers',
  entity_name:   'providers',
  entity_group:  'Reference',
  silver_table:  'silver_providers',
  description:   'provider_id PK, provider_name, npi, department, specialty',
  source_system: 'EHR'
});

// ── Transactional Entities ──────────────────────────────────
CREATE (e:Entity:Transactional {
  entity_id:     'encounters',
  entity_name:   'encounters',
  entity_group:  'Transactional',
  silver_table:  'silver_encounters',
  description:   'encounter_id PK, patient_id FK, provider_id FK, date_of_service, department, encounter_type',
  source_system: 'EHR'
});

CREATE (ch:Entity:Transactional {
  entity_id:     'charges',
  entity_name:   'charges',
  entity_group:  'Transactional',
  silver_table:  'silver_charges',
  description:   'charge_id PK, encounter_id FK, cpt_code, charge_amount REAL, units INTEGER, service_date, post_date',
  source_system: 'EHR / Charge Capture'
});

CREATE (c:Entity:Transactional {
  entity_id:     'claims',
  entity_name:   'claims',
  entity_group:  'Transactional',
  silver_table:  'silver_claims',
  description:   'claim_id PK, encounter_id FK, patient_id FK, payer_id FK, date_of_service, total_charge_amount REAL, claim_status, is_clean_claim INTEGER',
  source_system: 'Clearinghouse'
});

CREATE (py:Entity:Transactional {
  entity_id:     'payments',
  entity_name:   'payments',
  entity_group:  'Transactional',
  silver_table:  'silver_payments',
  description:   'payment_id PK, claim_id FK, payer_id FK, payment_amount REAL, allowed_amount REAL, is_accurate_payment INTEGER',
  source_system: 'Clearinghouse / ERA'
});

CREATE (d:Entity:Transactional {
  entity_id:     'denials',
  entity_name:   'denials',
  entity_group:  'Transactional',
  silver_table:  'silver_denials',
  description:   'denial_id PK, claim_id FK, denial_reason_code, denied_amount REAL, appeal_status, recovered_amount REAL',
  source_system: 'Clearinghouse / ERA'
});

CREATE (a:Entity:Transactional {
  entity_id:     'adjustments',
  entity_name:   'adjustments',
  entity_group:  'Transactional',
  silver_table:  'silver_adjustments',
  description:   'adjustment_id PK, claim_id FK, adjustment_type_code, adjustment_amount REAL',
  source_system: 'Billing System'
});

// ── Operational Entities ────────────────────────────────────
CREATE (oc:Entity:Operational {
  entity_id:     'operating_costs',
  entity_name:   'operating costs',
  entity_group:  'Operational',
  silver_table:  'silver_operating_costs',
  description:   'period PK, billing_staff_cost REAL, software_cost REAL, outsourcing_cost REAL, supplies_overhead REAL, total_rcm_cost REAL',
  source_system: 'ERP / Finance'
});

// ── Foreign-Key Relationships ───────────────────────────────
// payers → patients (primary_payer_id)
MATCH (parent:Entity {entity_id: 'payers'}), (child:Entity {entity_id: 'patients'})
CREATE (parent)-[:HAS_FK {
  join_column:      'primary_payer_id',
  cardinality:      '1:N',
  business_meaning: 'Each patient has one primary payer'
}]->(child);

// patients → encounters (patient_id)
MATCH (parent:Entity {entity_id: 'patients'}), (child:Entity {entity_id: 'encounters'})
CREATE (parent)-[:HAS_FK {
  join_column:      'patient_id',
  cardinality:      '1:N',
  business_meaning: 'A patient can have many visits'
}]->(child);

// providers → encounters (provider_id)
MATCH (parent:Entity {entity_id: 'providers'}), (child:Entity {entity_id: 'encounters'})
CREATE (parent)-[:HAS_FK {
  join_column:      'provider_id',
  cardinality:      '1:N',
  business_meaning: 'A provider sees many patients'
}]->(child);

// encounters → charges (encounter_id)
MATCH (parent:Entity {entity_id: 'encounters'}), (child:Entity {entity_id: 'charges'})
CREATE (parent)-[:HAS_FK {
  join_column:      'encounter_id',
  cardinality:      '1:N',
  business_meaning: 'Each visit generates line-item charges'
}]->(child);

// encounters → claims (encounter_id)
MATCH (parent:Entity {entity_id: 'encounters'}), (child:Entity {entity_id: 'claims'})
CREATE (parent)-[:HAS_FK {
  join_column:      'encounter_id',
  cardinality:      '1:N',
  business_meaning: 'Each visit produces one or more insurance claims'
}]->(child);

// payers → claims (payer_id)
MATCH (parent:Entity {entity_id: 'payers'}), (child:Entity {entity_id: 'claims'})
CREATE (parent)-[:HAS_FK {
  join_column:      'payer_id',
  cardinality:      '1:N',
  business_meaning: 'Claims are billed to one payer'
}]->(child);

// claims → payments (claim_id)
MATCH (parent:Entity {entity_id: 'claims'}), (child:Entity {entity_id: 'payments'})
CREATE (parent)-[:HAS_FK {
  join_column:      'claim_id',
  cardinality:      '1:N',
  business_meaning: 'A claim may receive partial or split payments'
}]->(child);

// claims → denials (claim_id)
MATCH (parent:Entity {entity_id: 'claims'}), (child:Entity {entity_id: 'denials'})
CREATE (parent)-[:HAS_FK {
  join_column:      'claim_id',
  cardinality:      '1:N',
  business_meaning: 'A claim can be denied once or multiple times'
}]->(child);

// claims → adjustments (claim_id)
MATCH (parent:Entity {entity_id: 'claims'}), (child:Entity {entity_id: 'adjustments'})
CREATE (parent)-[:HAS_FK {
  join_column:      'claim_id',
  cardinality:      '1:N',
  business_meaning: 'Contractual write-offs are applied per claim'
}]->(child);
