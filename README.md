# LeaseLens AI

LeaseLens AI is a multi-agent retail expansion assistant for Bangalore. It helps founders and expansion teams shortlist promising areas, review demand and accessibility signals, and turn location insight into next-step execution plans.

## What the prototype does

- Generates Bangalore retail recommendations by business type, budget, and target customer
- Supports area and pincode based search with autocomplete
- Returns a ranked shortlist with summary, positioning, accessibility, and Google Maps links
- Adds a coordinator brief, next-step plan, and decision checklist for expansion workflows

## Google Cloud stack

- Cloud Run for frontend and backend deployment
- BigQuery for serving retail location intelligence and business profiles
- ADK agent backend for retail expansion assistant workflows
- Cloud Build triggers for auto-deploy on pushes to `main`

## Services

- Frontend UI: `lease-lens-ui`
- Backend agent service: `lease-lens-ai`

## Deployment

This repo includes:

- `cloudbuild-ui.yaml` for the UI Cloud Run deployment
- `cloudbuild-backend.yaml` for the backend Cloud Run deployment

With Cloud Build triggers configured on the `main` branch, pushes to GitHub can automatically build and deploy both services.
