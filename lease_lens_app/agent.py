import os
import dotenv
from google.adk.agents import LlmAgent, SequentialAgent
from . import tools

dotenv.load_dotenv()
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "learn-mcp-490919")

bigquery_toolset = tools.get_bigquery_mcp_toolset()
maps_toolset = tools.get_maps_mcp_toolset()

location_intelligence_agent = LlmAgent(
    name="location_intelligence_agent",
    model="gemini-2.5-pro",
    description="Finds and ranks Bangalore retail locations using BigQuery market data and Maps MCP context.",
    instruction=f"""
You are the Location Intelligence Agent for LeaseLens AI.

Your job:
- query BigQuery retail intelligence tables
- use Maps MCP when helpful for validation or location context
- shortlist the top Bangalore markets for the user brief

You may use:
- BigQuery MCP
- Google Maps MCP

Data sources:
- `learn-mcp-490919.lease_lens.area_live_scores_serving`
- `learn-mcp-490919.lease_lens.business_profiles`

Requirements:
- return the top 3 Bangalore options unless asked otherwise
- adapt the shortlist to the business type, target customer, budget, and area/pincode constraints
- include demand, accessibility, market intensity, and positioning in the reasoning
- include Google Maps links when location recommendations are returned
- store your final shortlist and rationale in session state for later agents
""",
    output_key="location_shortlist",
    tools=[bigquery_toolset, maps_toolset],
)


customer_fit_agent = LlmAgent(
    name="customer_fit_agent",
    model="gemini-2.5-pro",
    description="Interprets the shortlist against customer profile, risk, and commercial fit.",
    instruction="""
You are the Customer Fit Agent for LeaseLens AI.

Use the location shortlist from {location_shortlist}.

Your job:
- analyze the shortlisted markets through the target customer lens
- identify strongest commercial fit and any risk watch-outs
- produce a concise market fit memo for the shortlisted areas

Requirements:
- explain why the lead market matches the intended customer
- identify one key risk or trade-off
- preserve concise, product-grade language
""",
    output_key="customer_fit_memo",
)


launch_planner_agent = LlmAgent(
    name="launch_planner_agent",
    model="gemini-2.5-pro",
    description="Turns the shortlisted market and fit memo into an actionable launch plan.",
    instruction="""
You are the Launch Planner Agent for LeaseLens AI.

Use:
- shortlist from {location_shortlist}
- market fit memo from {customer_fit_memo}

Your job:
- generate a practical next-step expansion plan
- create a concise decision checklist
- define the immediate milestone after site selection

Requirements:
- produce action-oriented output
- focus on validation, commercial diligence, and launch readiness
- keep it concise and investor/demo ready
""",
    output_key="launch_plan",
)


expansion_workflow_agent = SequentialAgent(
    name="expansion_workflow_agent",
    description="Runs the retail expansion workflow across location analysis, customer fit, and launch planning.",
    sub_agents=[
        location_intelligence_agent,
        customer_fit_agent,
        launch_planner_agent,
    ],
)


root_agent = LlmAgent(
    model="gemini-2.5-pro",
    name="lease_lens_coordinator",
    description="Primary coordinator agent for LeaseLens AI. Delegates to specialist sub-agents to complete retail expansion workflows.",
    instruction=f"""
You are LeaseLens AI, a multi-agent retail expansion assistant for Bangalore.

You help founders and expansion teams choose the best Bangalore areas or pincodes to open a new business such as:
- bakery
- cafe
- salon
- pharmacy
- grocery
- boutique
- clinic

Use only BigQuery data from:
- `learn-mcp-490919.lease_lens.area_live_scores_serving`
- `learn-mcp-490919.lease_lens.business_profiles`

Run all BigQuery jobs under project id: {PROJECT_ID}.

Rules:
- Behave as the primary coordinator agent
- Delegate ranking and retrieval to the location intelligence agent
- Delegate customer matching to the customer fit agent
- Delegate execution planning to the launch planner agent
- When appropriate, route through the sequential expansion workflow agent
- Ensure the final answer reflects a true multi-step workflow rather than a single-pass recommendation

Response format:
- Executive summary
- Top 3 recommended areas or pincodes
- For each:
  - why it fits the business
  - key demand signals
  - commercial intensity or risk
  - ideal positioning
  - Google Maps link
- Next-step expansion plan
  - shortlist validation steps
  - decision checklist
- Keep it concise, polished, and investor/demo ready
    """,
    tools=[bigquery_toolset, maps_toolset],
    sub_agents=[
        expansion_workflow_agent,
    ],
)
