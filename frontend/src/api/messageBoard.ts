import { apiRequest } from "./client";

export interface MessageBoardEntryCreateRequest {
    request_name: string;
    detail: string;
    system_name: string;
    expected_completion_date?: string | null;
}

export interface MessageBoardEntryCreateResponse {
    id: number;
    status: string;
    created_at: string;
    message: string;
}

export function createMessageBoardEntry(payload: MessageBoardEntryCreateRequest) {
    return apiRequest<MessageBoardEntryCreateResponse>("/api/message-board/entries", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
    });
}
