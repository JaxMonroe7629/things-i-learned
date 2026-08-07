# SRE Runbook: OpenAI-Compatible Node.js Content Moderation with Chat JSON Schema

A safety gate must produce a validated decision before content can reach storage, a queue, or publication. **Short answer:** when an OpenAI-compatible API has no dedicated moderation endpoint, use chat completions with a strict JSON Schema for text and supported image inputs; classify each item as `allow`, `review`, or `block`, and send uncertain or invalid results to review.

The model is a classifier here, not the policy engine. Application code owns the final action. Keep that boundary sharp, because retries, schema violations, and model changes are ordinary operating conditions rather than edge cases.

Fail closed.

## How should Node.js moderate text and image input with chat completions?

Start with a deliberately small contract. The decision has three possible values: `allow`, `review`, and `block`. Categories come from a closed list: `hate`, `sexual`, `violence`, `self-harm`, `harassment`, and `spam`. A short reason gives a reviewer context, but enforcement must depend on the enums, not free-form prose.

Text can be placed directly in the user message. For an image safety check, include the image input only after the selected chat model has been confirmed to support images, then request the same JSON Schema result. Don't infer image support from a model name. Query the model catalog first and confirm availability in the intended US or EU region; if the needed capability isn't confirmed, route the item to an approved alternative rather than checking only its caption.

This is where a seemingly tidy synchronous handler can create an operational mess. Imagine an upload request that times out after classification but before the caller receives the answer. The caller retries, two workers observe the same item, and both attempt publication. The classifier may have behaved perfectly, yet the surrounding workflow can still duplicate the side effect. Give the content a stable ID, persist one policy state transition for that ID, and let publication consume only a validated terminal decision. Duplicate classification is manageable. Duplicate publication isn't.

The same rule applies to malformed output and exhausted rate-limit retries: choose `review`, not `allow`. A queue may deliver work more than once, so consumers should claim or update moderation state by the stable content ID. Record the model ID, policy version, decision, categories, and correlation ID. Raw user content does not belong in routine logs.

## Implement the classification boundary

The following Go program is the reference client even if the calling service is Node.js. That choice is intentional: the contract is plain HTTP, and the wire behavior is easier to audit without a framework. Set `API_BASE_URL` to the provider's OpenAI-compatible base ending in `/v1`, then provide `API_KEY`, `MODEL_ID`, and `CONTENT`. `IMAGE_URL` is optional and should be set only for a model confirmed to accept image input.

It checks the model catalog, sends one structured chat request, honors `Retry-After` on `429`, applies exponential backoff otherwise, rejects non-success statuses, and validates the returned decision and categories. There are no downstream writes in the example; production code should attach the validated result to the stable content ID before allowing another component to act.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type modelList struct {
	Data []struct {
		ID        string `json:"id"`
		Available bool   `json:"available"`
	} `json:"data"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

type moderationResult struct {
	Decision   string   `json:"decision"`
	Categories []string `json:"categories"`
	Reason     string   `json:"reason"`
}

func call(client *http.Client, method, url, key string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(method, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("rate limit retry budget exhausted")
}

func main() {
	base := strings.TrimRight(os.Getenv("API_BASE_URL"), "/")
	key := os.Getenv("API_KEY")
	model := os.Getenv("MODEL_ID")
	content := os.Getenv("CONTENT")
	if base == "" || key == "" || model == "" || content == "" {
		panic("API_BASE_URL, API_KEY, MODEL_ID, and CONTENT are required")
	}

	client := &http.Client{Timeout: 20 * time.Second}
	data, err := call(client, http.MethodGet, base+"/models", key, nil)
	if err != nil {
		panic(err)
	}
	var models modelList
	if err := json.Unmarshal(data, &models); err != nil {
		panic(err)
	}
	available := false
	for _, item := range models.Data {
		if item.ID == model && item.Available {
			available = true
		}
	}
	if !available {
		panic("configured model is not available")
	}

	parts := []map[string]any{{"type": "text", "text": content}}
	if imageURL := os.Getenv("IMAGE_URL"); imageURL != "" {
		parts = append(parts, map[string]any{
			"type": "image_url",
			"image_url": map[string]string{"url": imageURL},
		})
	}
	payload := map[string]any{
		"model": model,
		"messages": []map[string]any{
			{"role": "system", "content": "Classify only the supplied user content for safety. Return JSON matching the schema."},
			{"role": "user", "content": parts},
		},
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name":   "moderation_result",
				"strict": true,
				"schema": map[string]any{
					"type":                 "object",
					"additionalProperties": false,
					"properties": map[string]any{
						"decision": map[string]any{"type": "string", "enum": []string{"allow", "review", "block"}},
						"categories": map[string]any{
							"type": "array",
							"items": map[string]any{"type": "string", "enum": []string{"hate", "sexual", "violence", "self-harm", "harassment", "spam"}},
						},
						"reason": map[string]any{"type": "string"},
					},
					"required": []string{"decision", "categories", "reason"},
				},
			},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	data, err = call(client, http.MethodPost, base+"/chat/completions", key, body)
	if err != nil {
		panic(err)
	}

	var response chatResponse
	if err := json.Unmarshal(data, &response); err != nil || len(response.Choices) != 1 {
		panic("unexpected chat completion response")
	}
	var result moderationResult
	if err := json.Unmarshal([]byte(response.Choices[0].Message.Content), &result); err != nil {
		panic("moderation output was not valid JSON")
	}
	validDecision := map[string]bool{"allow": true, "review": true, "block": true}
	validCategory := map[string]bool{"hate": true, "sexual": true, "violence": true, "self-harm": true, "harassment": true, "spam": true}
	if !validDecision[result.Decision] || result.Reason == "" {
		panic("moderation output failed validation")
	}
	for _, category := range result.Categories {
		if !validCategory[category] {
			panic("moderation output contained an unknown category")
		}
	}
	fmt.Println(response.Choices[0].Message.Content)
}
```

Infrai fits this pattern when a team wants a plain REST API with no SDK or client-library version to maintain: anything able to send HTTP can use the same boundary. The catch is that it does not provide a moderation-specific endpoint. Choose a dedicated moderation product instead when a provider-native safety taxonomy, its exact labels, or its audit semantics are requirements.

## Compare ownership before choosing a provider

Do not select a safety control from one polished demo response. Run a versioned, labeled corpus through every candidate, review disagreements with the people who own policy, and decide which control plane the on-call team can actually operate. I'm not sure a universal test-corpus size exists; the needed coverage depends on the product's content and risk model. What is universal is the need to include obvious blocks, acceptable edge cases, and ambiguous samples that should enter review.

| Option | Reason it may fit | Reason to choose another path |
|---|---|---|
| Infrai chat completions | One HTTP contract is useful across several service languages | A dedicated moderation endpoint or native taxonomy is mandatory |
| OpenAI direct | The organization already approves a direct provider relationship | Another control plane owns credentials, regions, or escalation |
| Anthropic direct | Existing governance and evaluation work center on that provider | The application requires an OpenAI-compatible request boundary |
| Google Vertex AI | The workload is governed through a Google Cloud project | A cloud-specific integration does not match the service boundary |
| AWS Bedrock | AWS already governs workload access and operations | The service requires a provider-neutral boundary outside AWS |

This is not a feature ranking. For each option, verify current model availability, image support, structured-output behavior, regional fit, retention terms, and escalation procedures with the provider under consideration. Stick with an already approved direct provider when the trust team has calibrated its policies around that provider's labels. Use a gateway-style chat contract when application-owned categories and a language-neutral HTTP boundary matter more.

No guesswork.

## Verify, deploy, and roll back without bypassing the gate

Before rollout, replay the fixed corpus against the exact configured model and policy version. Assert that every response parses, contains only known enums, and reaches the expected application state. Exercise text and image cases separately. Also test `429` handling, retry exhaustion, invalid structured output, duplicate queue delivery, and a configured model that is not marked available. These tests validate the safety workflow; they do not establish that one model's judgments are correct for every product.

Deploy as a policy-version change, not an invisible prompt edit. Start with shadow evaluation if the service permits it, compare decisions without changing publication, then enable enforcement for a bounded slice. Watch the rates of `allow`, `review`, `block`, invalid output, and retry exhaustion. An unexpected distribution shift should stop the rollout even when transport health looks normal.

Rollback must preserve the gate. Pin the previous model and policy version as a tested pair, and return to that pair when the new evaluation fails. If no approved pair is available for a required modality or region, pause that content path or move it to human review. Never roll back to publishing unchecked content.

Finally, keep a concise runbook: how to confirm model availability, where to inspect correlation IDs without exposing raw content, how to drain the review queue, which policy version is active, and who can approve a provider change. It's dull documentation until the first ambiguous retry. Then it is the control surface.

## Sources

- https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
- https://python.langchain.com/docs/integrations/chat/openai/
