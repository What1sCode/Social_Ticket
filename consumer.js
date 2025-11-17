require('dotenv').config();
const { Pool } = require('pg');
const axios = require('axios');

// Environment variables
const ZENDESK_SUBDOMAIN = process.env.ZENDESK_SUBDOMAIN;
const ZENDESK_EMAIL = process.env.ZENDESK_EMAIL;
const ZENDESK_API_TOKEN = process.env.ZENDESK_API_TOKEN;
const DATABASE_URL = process.env.DATABASE_URL;
const CONSUMER_REFRESH_INTERVAL_MINUTES = parseInt(process.env.CONSUMER_REFRESH_INTERVAL_MINUTES || '5');
const FORCE_REFRESH = process.env.FORCE_REFRESH === 'true';

// Validation
if (!ZENDESK_SUBDOMAIN || !ZENDESK_EMAIL || !ZENDESK_API_TOKEN || !DATABASE_URL) {
  console.error('❌ Missing required environment variables:');
  console.error('   - ZENDESK_SUBDOMAIN');
  console.error('   - ZENDESK_EMAIL');
  console.error('   - ZENDESK_API_TOKEN');
  console.error('   - DATABASE_URL');
  process.exit(1);
}

// PostgreSQL Pool
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// Zendesk API client
const zendeskClient = axios.create({
  baseURL: `https://${ZENDESK_SUBDOMAIN}.zendesk.com`,
  auth: {
    username: `${ZENDESK_EMAIL}/token`,
    password: ZENDESK_API_TOKEN,
  },
});

// ==================== DATABASE FUNCTIONS ====================

/**
 * Get the last time we successfully polled events
 */
async function getLastRunTime() {
  try {
    const result = await pool.query(
      'SELECT last_run_at FROM consumer_state ORDER BY created_at DESC LIMIT 1'
    );
    
    if (result.rows.length === 0) {
      const defaultTime = new Date(Date.now() - 24 * 60 * 60 * 1000); // 24 hours ago
      console.log(`📅 No previous run found. Starting from 24 hours ago: ${defaultTime.toISOString()}`);
      return defaultTime;
    }
    
    return new Date(result.rows[0].last_run_at);
  } catch (error) {
    console.error('❌ Error fetching last run time:', error.message);
    return new Date(Date.now() - 24 * 60 * 60 * 1000);
  }
}

/**
 * Update consumer state after polling
 */
async function updateLastRunTime(status, totalStored, totalFailed, errorMessage = null) {
  try {
    await pool.query(
      `INSERT INTO consumer_state (last_run_at, run_status, error_message, total_stored, total_failed)
       VALUES (NOW(), $1, $2, $3, $4)`,
      [status, errorMessage, totalStored, totalFailed]
    );
    console.log(`✅ Updated consumer_state: status=${status}, stored=${totalStored}, failed=${totalFailed}`);
  } catch (error) {
    console.error('❌ Error updating consumer_state:', error.message);
  }
}

/**
 * Fetch ticket metadata from Zendesk
 */
async function fetchTicketMetadata(ticketId) {
  try {
    const response = await zendeskClient.get(`/api/v2/tickets/${ticketId}`);
    const ticket = response.data.ticket;
    
    return {
      subject: ticket.subject,
      priority: ticket.priority,
      status: ticket.status,
      created_at: ticket.created_at,
    };
  } catch (error) {
    console.warn(`⚠️ Failed to fetch ticket #${ticketId}: ${error.message}`);
    return {
      subject: null,
      priority: null,
      status: null,
      created_at: null,
    };
  }
}

/**
 * Fetch agent/user metadata from Zendesk
 */
async function fetchAgentMetadata(userId) {
  try {
    const response = await zendeskClient.get(`/api/v2/users/${userId}`);
    const user = response.data.user;
    
    return {
      id: user.id,
      name: user.name,
      email: user.email,
    };
  } catch (error) {
    console.warn(`⚠️ Failed to fetch user #${userId}: ${error.message}`);
    return {
      id: userId,
      name: 'Unknown Agent',
      email: null,
    };
  }
}

/**
 * Store a single ticket view event
 */
async function storeViewEvent(event) {
  try {
    const ticketId = event.ticket?.id;
    const agentId = event.actor?.id;
    const viewedAt = event.created_at;
    const eventId = event.id;

    if (!ticketId || !agentId || !viewedAt) {
      console.warn(`⚠️ Incomplete event data: ticketId=${ticketId}, agentId=${agentId}, viewedAt=${viewedAt}`);
      return false;
    }

    // Fetch metadata (errors are logged but don't crash)
    const ticketMeta = await fetchTicketMetadata(ticketId);
    const agentMeta = await fetchAgentMetadata(agentId);

    // Insert with ON CONFLICT to handle duplicates
    const result = await pool.query(
      `INSERT INTO ticket_views 
       (event_id, ticket_id, agent_id, agent_name, agent_email, ticket_subject, ticket_priority, ticket_status, ticket_created_at, viewed_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
       ON CONFLICT (event_id) DO NOTHING`,
      [
        eventId,
        ticketId,
        agentId,
        agentMeta.name,
        agentMeta.email,
        ticketMeta.subject,
        ticketMeta.priority,
        ticketMeta.status,
        ticketMeta.created_at,
        viewedAt,
      ]
    );

    if (result.rowCount === 0) {
      console.log(`📋 Event ${eventId} already stored (duplicate)`);
      return false;
    }

    console.log(`✅ Stored event: ticket #${ticketId}, agent: ${agentMeta.name}`);
    return true;
  } catch (error) {
    console.error(`❌ Error storing view event: ${error.message}`);
    return false;
  }
}

/**
 * Main polling loop - fetch events from Zendesk API
 */
async function pollEvents() {
  console.log('\n🔄 Starting polling cycle...');
  
  try {
    let lastRunTime = await getLastRunTime();
    
    // Allow force refresh to override
    if (FORCE_REFRESH) {
      lastRunTime = new Date(Date.now() - 24 * 60 * 60 * 1000);
      console.log('🔄 FORCE_REFRESH enabled: fetching 24 hours back');
    }

    const unixTimestamp = Math.floor(lastRunTime.getTime() / 1000);
    console.log(`📅 Querying events since: ${lastRunTime.toISOString()} (${unixTimestamp})`);

    // Query Zendesk Events API
    const response = await zendeskClient.get('/api/v2/events', {
      params: {
        'filter[type]': 'ticket.view',
        'filter[created_after]': unixTimestamp,
      },
    });

    const events = response.data.events || [];
    console.log(`📊 Found ${events.length} ticket view events`);

    // Process each event
    let successCount = 0;
    let failureCount = 0;

    for (const event of events) {
      const stored = await storeViewEvent(event);
      if (stored) {
        successCount++;
      } else {
        failureCount++;
      }
    }

    console.log(`📈 Poll summary: Stored: ${successCount}, Failed: ${failureCount}, Total: ${events.length}`);

    // Update consumer state
    await updateLastRunTime('success', successCount, failureCount);

    return { successCount, failureCount, totalEvents: events.length };
  } catch (error) {
    console.error(`❌ Poll cycle failed: ${error.message}`);
    await updateLastRunTime('error', 0, 0, error.message);
    throw error;
  }
}

/**
 * Initialize and start consumer
 */
async function main() {
  console.log('🚀 Zendesk Events API Consumer starting...');
  console.log(`   Subdomain: ${ZENDESK_SUBDOMAIN}`);
  console.log(`   Email: ${ZENDESK_EMAIL}`);
  console.log(`   Interval: ${CONSUMER_REFRESH_INTERVAL_MINUTES} minutes`);
  console.log(`   Force Refresh: ${FORCE_REFRESH}`);

  // Test database connection
  try {
    await pool.query('SELECT NOW()');
    console.log('✅ Database connection successful');
  } catch (error) {
    console.error('❌ Database connection failed:', error.message);
    process.exit(1);
  }

  // Run first poll immediately
  try {
    await pollEvents();
  } catch (error) {
    console.error('❌ Initial poll failed:', error.message);
  }

  // Schedule recurring polls
  const intervalMs = CONSUMER_REFRESH_INTERVAL_MINUTES * 60 * 1000;
  console.log(`⏰ Scheduling next poll in ${CONSUMER_REFRESH_INTERVAL_MINUTES} minutes`);

  setInterval(async () => {
    try {
      await pollEvents();
    } catch (error) {
      console.error('❌ Scheduled poll failed:', error.message);
    }
  }, intervalMs);

  // Graceful shutdown
  process.on('SIGTERM', async () => {
    console.log('🛑 SIGTERM received - shutting down gracefully');
    await pool.end();
    process.exit(0);
  });

  process.on('SIGINT', async () => {
    console.log('🛑 SIGINT received - shutting down gracefully');
    await pool.end();
    process.exit(0);
  });
}

// Start the service
main().catch(error => {
  console.error('❌ Fatal error:', error);
  process.exit(1);
});
```

---

## **.env.example - Environment Variables**
```
ZENDESK_SUBDOMAIN=elotouchcare
ZENDESK_EMAIL=roger.rhodes@elotouch.com
ZENDESK_API_TOKEN=your_api_token_here
DATABASE_URL=postgresql://user:password@localhost:5432/ticket_views
CONSUMER_REFRESH_INTERVAL_MINUTES=5
FORCE_REFRESH=false
