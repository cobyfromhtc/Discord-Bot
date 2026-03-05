# -*- coding: utf-8 -*-
import signal
import discord
from discord.ext import commands, tasks
import discord.utils
import asyncio
import os
import logging
import sys

# Force UTF-8 output on all platforms (fixes garbled emoji/symbols on Windows)
try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass
from datetime import datetime, timedelta, timezone
import random
import json
from discord.ui import Button, View, Modal, TextInput, Select
from discord import app_commands
import time
from logging.handlers import RotatingFileHandler
from typing import Optional, Set, Dict, Tuple, List, Any
import threading
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import sqlite3
import pickle

try:
    import aiosqlite
    SQLITE_AVAILABLE = True
except ImportError:
    SQLITE_AVAILABLE = False

try:
    from flask import Flask
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False

if FLASK_AVAILABLE:
    app = Flask(__name__)
    
    # Configuration
    SECRET_KEY = "VPRP_MOS_2026"  # Must match Roblox script
    verification_codes = {}  # Store codes: {code: {roblox_user_id, roblox_username, timestamp}}
    
    @app.route('/store_code', methods=['POST'])
    def store_code():
        from flask import request, jsonify
        data = request.get_json()
        
        # Verify secret key
        if not data or data.get('secret') != SECRET_KEY:
            return jsonify({'error': 'Unauthorized'}), 401
        
        # Check required fields (matches Roblox script)
        if not data.get('code') or not data.get('roblox_username'):
            return jsonify({'error': 'Missing code or username'}), 400
        
        verification_codes[data['code']] = {
            'roblox_user_id': data.get('roblox_user_id'),
            'roblox_username': data.get('roblox_username'),
            'timestamp': data.get('timestamp')
        }
        
        logging.info(f"[Roblox] Stored code {data['code']} for {data['roblox_username']}")
        return jsonify({'success': True}), 200
    
    @app.route('/invalidate_code', methods=['POST'])
    def invalidate_code():
        from flask import request, jsonify
        data = request.get_json()
        
        # Verify secret key
        if not data or data.get('secret') != SECRET_KEY:
            return jsonify({'error': 'Unauthorized'}), 401
        
        roblox_user_id = data.get('roblox_user_id')
        
        # Remove any codes for this user
        codes_to_remove = [code for code, info in verification_codes.items() 
                           if info.get('roblox_user_id') == roblox_user_id]
        
        for code in codes_to_remove:
            del verification_codes[code]
        
        logging.info(f"[Roblox] Invalidated {len(codes_to_remove)} codes for user {roblox_user_id}")
        return jsonify({'success': True, 'invalidated': len(codes_to_remove)}), 200
    
    def run_web():
        app.run(host='0.0.0.0', port=5000)


# --- ENUMERATIONS ---
class VerificationStatus(Enum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    PENDING_INFO = "pending_info"


class TicketStatus(Enum):
    OPEN = "open"
    IN_PROGRESS = "in_progress"
    RESOLVED = "resolved"
    CLOSED = "closed"


class GiveawayStatus(Enum):
    ACTIVE = "active"
    ENDED = "ended"
    CANCELLED = "cancelled"


class WarningType(Enum):
    SPAM = "spam"
    HARASSMENT = "harassment"
    TOXIC = "toxic"
    RAID = "raid"
    ADVERTISING = "advertising"
    NSFW = "nsfw"
    CUSTOM = "custom"


# --- CONFIGURATION ---
@dataclass
class ChannelConfig:
    invite: int = 1471663457287012497
    welcome: int = 1471663455017767107
    rules: int = 1471663457287012497
    log: int = 1471663485934108694
    auto_scan: int = 1471663527247741001
    verification_main: int = 1471663452677476520
    verification_submission: int = 1471663454115860611
    reports: int = 1329157448564609056
    tickets: int = 1471663451561525522 # TICKET CATEGORY (DO NOT CHANGE)
    transcripts: int = 1471663530238546093 # TRANSCRIPTS CHANNEL
    giveaways: int = 1471663463599309066


@dataclass
class RoleConfig:
    invite_manager: int = 1471663385568350282
    member: int = 1471663405877166090
    staff: int = 1471663388848423087
    verified: int = 1411440990408937642
    verification_ping: int = 1476162221632389220
    muted: int = 0
    ticket_support: int = 0


@dataclass
class ServerConfig:
    mos_server_id: int = 1471662897397633034
    mos_rules_channel: int = 1471663457287012497
    mos_rules_message: int = 1473423973797593198
    vprp_server_id: int = 1163937669068357844
    vprp_rules_channel: int = 1163938532964978718
    vprp_rules_message: int = 1453140236732469258


@dataclass
class RobloxConfig:
    verification_game_url: str = "https://www.roblox.com/games/121601123290942/MOS-Roblox-Account-Verification"  # Replace with your game URL


@dataclass
class TimingConfig:
    invite_check_interval_minutes: int = 5
    auto_scan_interval_hours: int = 2
    report_message_interval_minutes: int = 30
    verification_timeout_seconds: int = 180
    confirmation_timeout_seconds: int = 60
    report_timeout_seconds: int = 300
    info_request_timeout_seconds: int = 300


@dataclass
class LimitsConfig:
    min_account_age_days: int = 6
    min_blacklist_keyword_length: int = 2
    min_poll_options: int = 2
    max_poll_options: int = 10
    min_poll_duration: int = 30
    max_poll_duration: int = 86400
    max_warnings_before_ban: int = 3
    max_tickets_per_user: int = 3
    max_giveaway_winners: int = 10


class Config:
    def __init__(self):
        self.channels = ChannelConfig()
        self.roles = RoleConfig()
        self.servers = ServerConfig()
        self.timing = TimingConfig()
        self.limits = LimitsConfig()
        self.roblox = RobloxConfig()  # ← ADD THIS LINE

        self.data_dir = "data"
        self.json_dir = "data/JsonData"
        self.db_file = "data/bot_data.db"
        self.lock_file = "bot_busy.lock"

        self.tickets_data_file = "data/JsonData/tickets_data.json"
        self.giveaways_data_file = "data/JsonData/giveaways_data.json"

        self.enable_leveling = True
        self.enable_tickets = True
        self.enable_giveaways = True
        self.enable_warnings = True

        self.command_prefix = "!"
        self.bot_status = "MOS On Top"
        self.debug_mode = False

    def ensure_directories(self) -> None:
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.json_dir, exist_ok=True)


# --- SQLITE DATA MANAGER (For persistent data - saves PC resources) ---
class DataManager:
    """
    SQLite-based data manager for persistent data.
    More efficient than JSON for frequent read/write operations.
    Stores: invites, blacklist, rules_cache, levels, warnings
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._connection: Optional[sqlite3.Connection] = None
        self._lock = threading.Lock()

    def connect(self) -> None:
        self._connection = sqlite3.connect(self.db_path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._create_tables()
        logging.info(f"[DataManager] Connected to database: {self.db_path}")

    def _create_tables(self) -> None:
        cursor = self._connection.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS invites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER,
                channel_id INTEGER,
                tracked_invites BLOB
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS blacklist (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                keywords BLOB
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rules_cache (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                mos_rules TEXT,
                vprp_rules TEXT,
                mos_last_updated TEXT,
                vprp_last_updated TEXT
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS levels (
                user_id INTEGER,
                guild_id INTEGER,
                xp INTEGER DEFAULT 0,
                level INTEGER DEFAULT 0,
                total_messages INTEGER DEFAULT 0,
                last_xp_gain TEXT,
                PRIMARY KEY (user_id, guild_id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS warnings (
                warning_id TEXT PRIMARY KEY,
                user_id INTEGER,
                guild_id INTEGER,
                moderator_id INTEGER,
                warning_type TEXT,
                reason TEXT,
                points INTEGER DEFAULT 1,
                created_at TEXT,
                expires_at TEXT,
                is_active INTEGER DEFAULT 1
            )
        ''')

        # =============================================================================
        # TICKET TOOL TABLES (Full Ticket Tool Clone)
        # =============================================================================

        # Ticket Panels - Store panel configurations
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ticket_panels (
                panel_id TEXT PRIMARY KEY,
                guild_id INTEGER,
                channel_id INTEGER,
                message_id INTEGER,
                name TEXT,
                description TEXT,
                embed_title TEXT,
                embed_description TEXT,
                embed_color INTEGER,
                embed_image TEXT,
                embed_thumbnail TEXT,
                button_style INTEGER,
                button_label TEXT,
                button_emoji TEXT,
                category_id INTEGER,
                support_role_id INTEGER,
                ticket_limit INTEGER DEFAULT 1,
                auto_close_hours INTEGER DEFAULT 24,
                welcome_message TEXT,
                claim_required INTEGER DEFAULT 0,
                created_at TEXT,
                is_active INTEGER DEFAULT 1
            )
        ''')

        # Tickets - Store individual ticket data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tickets (
                ticket_id TEXT PRIMARY KEY,
                guild_id INTEGER,
                channel_id INTEGER,
                panel_id TEXT,
                creator_id INTEGER,
                category TEXT,
                subject TEXT,
                claimed_by INTEGER,
                claimed_at TEXT,
                status TEXT DEFAULT 'open',
                created_at TEXT,
                closed_at TEXT,
                closed_by INTEGER,
                close_reason TEXT,
                rating INTEGER,
                rating_feedback TEXT
            )
        ''')

        # Ticket Transcripts - Store transcript data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ticket_transcripts (
                transcript_id TEXT PRIMARY KEY,
                ticket_id TEXT,
                guild_id INTEGER,
                channel_id INTEGER,
                creator_id INTEGER,
                closed_by INTEGER,
                claimed_by INTEGER,
                category TEXT,
                created_at TEXT,
                closed_at TEXT,
                message_count INTEGER,
                file_path TEXT,
                html_content TEXT
            )
        ''')

        # Ticket Blacklist - Users blocked from creating tickets
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ticket_blacklist (
                blacklist_id TEXT PRIMARY KEY,
                guild_id INTEGER,
                user_id INTEGER,
                reason TEXT,
                blacklisted_by INTEGER,
                blacklisted_at TEXT,
                expires_at TEXT,
                is_active INTEGER DEFAULT 1
            )
        ''')

        # Ticket Settings - Per-guild ticket settings
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ticket_settings (
                guild_id INTEGER PRIMARY KEY,
                category_id INTEGER,
                transcripts_channel_id INTEGER,
                log_channel_id INTEGER,
                support_role_id INTEGER,
                admin_role_id INTEGER,
                max_tickets_per_user INTEGER DEFAULT 3,
                auto_close_hours INTEGER DEFAULT 24,
                mention_on_create INTEGER DEFAULT 1,
                dm_transcripts INTEGER DEFAULT 1,
                require_claim INTEGER DEFAULT 0,
                created_at TEXT,
                updated_at TEXT
            )
        ''')

        # Ticket Questions - Custom questions for panels
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ticket_questions (
                question_id TEXT PRIMARY KEY,
                panel_id TEXT,
                guild_id INTEGER,
                question_text TEXT,
                question_type TEXT DEFAULT 'text',
                required INTEGER DEFAULT 1,
                placeholder TEXT,
                order_index INTEGER,
                created_at TEXT
            )
        ''')

        # Ticket Answers - Store user answers to questions
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ticket_answers (
                answer_id TEXT PRIMARY KEY,
                ticket_id TEXT,
                question_id TEXT,
                user_id INTEGER,
                answer_text TEXT,
                answered_at TEXT
            )
        ''')

        # Ticket Messages - Track messages for transcripts
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ticket_messages (
                message_id INTEGER PRIMARY KEY,
                ticket_id TEXT,
                author_id INTEGER,
                author_name TEXT,
                author_avatar TEXT,
                content TEXT,
                attachments TEXT,
                created_at TEXT
            )
        ''')

        # Ticket Notes - Private staff-only notes per ticket
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ticket_notes (
                note_id TEXT PRIMARY KEY,
                ticket_id TEXT,
                guild_id INTEGER,
                author_id INTEGER,
                content TEXT,
                created_at TEXT
            )
        ''')

        # === MIGRATIONS: add new columns to existing tables safely ===
        migrations = [
            'ALTER TABLE tickets ADD COLUMN priority TEXT DEFAULT "normal"',
            'ALTER TABLE tickets ADD COLUMN first_response_at TEXT',
            'ALTER TABLE ticket_settings ADD COLUMN sla_hours INTEGER DEFAULT 0',
            'ALTER TABLE ticket_panels ADD COLUMN sla_hours INTEGER DEFAULT 0',
        ]
        for sql in migrations:
            try:
                cursor.execute(sql)
            except Exception:
                pass  # Column already exists - safe to ignore

        self._connection.commit()

    def close(self) -> None:
        if self._connection:
            self._connection.close()
            logging.info("[DataManager] Database connection closed")

    # === INVITES ===
    def save_invites(self, message_id: Optional[int], channel_id: Optional[int], tracked_invites: Dict) -> None:
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute('DELETE FROM invites')
            cursor.execute(
                'INSERT INTO invites (message_id, channel_id, tracked_invites) VALUES (?, ?, ?)',
                (message_id, channel_id, pickle.dumps(tracked_invites))
            )
            self._connection.commit()

    def load_invites(self) -> Tuple[Optional[int], Optional[int], Dict]:
        cursor = self._connection.cursor()
        cursor.execute('SELECT message_id, channel_id, tracked_invites FROM invites LIMIT 1')
        row = cursor.fetchone()
        if row:
            tracked = pickle.loads(row['tracked_invites']) if row['tracked_invites'] else {}
            return row['message_id'], row['channel_id'], tracked
        return None, None, {}

    # === BLACKLIST ===
    def save_blacklist(self, keywords: Set[str]) -> None:
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute('DELETE FROM blacklist')
            cursor.execute('INSERT INTO blacklist (id, keywords) VALUES (1, ?)', (pickle.dumps(keywords),))
            self._connection.commit()

    def load_blacklist(self) -> Set[str]:
        cursor = self._connection.cursor()
        cursor.execute('SELECT keywords FROM blacklist WHERE id = 1')
        row = cursor.fetchone()
        return pickle.loads(row['keywords']) if row and row['keywords'] else set()

    # === RULES CACHE ===
    def save_rules_cache(self, cache: Dict) -> None:
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute('DELETE FROM rules_cache')
            cursor.execute('''
                INSERT INTO rules_cache (id, mos_rules, vprp_rules, mos_last_updated, vprp_last_updated)
                VALUES (1, ?, ?, ?, ?)
            ''', (
                cache.get('mos_rules', ''),
                cache.get('vprp_rules', ''),
                cache.get('mos_last_updated'),
                cache.get('vprp_last_updated')
            ))
            self._connection.commit()

    @staticmethod
    def _fix_mojibake(text: str) -> str:
        """
        Fix text that was stored as UTF-8 but read/saved as cp1252 (Windows encoding bug).
        Converts garbled characters like 'âž¢' back to their correct form '➢'.
        Safe to call on already-correct text - it will return it unchanged.
        """
        if not text:
            return text
        try:
            return text.encode('cp1252').decode('utf-8')
        except (UnicodeEncodeError, UnicodeDecodeError):
            return text  # Already correct UTF-8, leave it alone

    def load_rules_cache(self) -> Dict:
        cursor = self._connection.cursor()
        cursor.execute('SELECT mos_rules, vprp_rules, mos_last_updated, vprp_last_updated FROM rules_cache WHERE id = 1')
        row = cursor.fetchone()
        if row:
            mos_rules = self._fix_mojibake(row['mos_rules'] or '')
            vprp_rules = self._fix_mojibake(row['vprp_rules'] or '')
            self.save_rules_cache({
                'mos_rules': mos_rules,
                'vprp_rules': vprp_rules,
                'mos_last_updated': row['mos_last_updated'],
                'vprp_last_updated': row['vprp_last_updated']
            })
            return {
                'mos_rules': mos_rules,
                'vprp_rules': vprp_rules,
                'mos_last_updated': row['mos_last_updated'],
                'vprp_last_updated': row['vprp_last_updated']
            }
        return {'mos_rules': '', 'vprp_rules': '', 'mos_last_updated': None, 'vprp_last_updated': None}

    # === LEVELS ===
    def save_level(self, user_id: int, guild_id: int, xp: int, level: int, total_messages: int, last_xp_gain: Optional[datetime]) -> None:
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO levels (user_id, guild_id, xp, level, total_messages, last_xp_gain)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (user_id, guild_id, xp, level, total_messages, last_xp_gain.isoformat() if last_xp_gain else None))
            self._connection.commit()

    def load_level(self, user_id: int, guild_id: int) -> Optional[Dict]:
        cursor = self._connection.cursor()
        cursor.execute('SELECT xp, level, total_messages, last_xp_gain FROM levels WHERE user_id = ? AND guild_id = ?',
                      (user_id, guild_id))
        row = cursor.fetchone()
        if row:
            return {
                'xp': row['xp'],
                'level': row['level'],
                'total_messages': row['total_messages'],
                'last_xp_gain': datetime.fromisoformat(row['last_xp_gain']) if row['last_xp_gain'] else None
            }
        return None

    def load_all_levels(self) -> Dict[Tuple[int, int], Dict]:
        cursor = self._connection.cursor()
        cursor.execute('SELECT user_id, guild_id, xp, level, total_messages, last_xp_gain FROM levels')
        result = {}
        for row in cursor.fetchall():
            result[(row['user_id'], row['guild_id'])] = {
                'xp': row['xp'],
                'level': row['level'],
                'total_messages': row['total_messages'],
                'last_xp_gain': datetime.fromisoformat(row['last_xp_gain']) if row['last_xp_gain'] else None
            }
        return result

    def save_all_levels(self, levels: Dict[Tuple[int, int], Dict]) -> None:
        with self._lock:
            cursor = self._connection.cursor()
            for (user_id, guild_id), data in levels.items():
                cursor.execute('''
                    INSERT OR REPLACE INTO levels (user_id, guild_id, xp, level, total_messages, last_xp_gain)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    user_id, guild_id,
                    data.get('xp', 0),
                    data.get('level', 0),
                    data.get('total_messages', 0),
                    data['last_xp_gain'].isoformat() if data.get('last_xp_gain') else None
                ))
            self._connection.commit()

    # === WARNINGS ===
    def save_warning(self, warning: Dict) -> None:
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO warnings
                (warning_id, user_id, guild_id, moderator_id, warning_type, reason, points, created_at, expires_at, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                warning.get('warning_id'),
                warning.get('user_id'),
                warning.get('guild_id'),
                warning.get('moderator_id'),
                warning.get('warning_type'),
                warning.get('reason'),
                warning.get('points', 1),
                warning.get('created_at'),
                warning.get('expires_at'),
                1 if warning.get('is_active', True) else 0
            ))
            self._connection.commit()

    def load_warnings(self, guild_id: Optional[int] = None) -> Dict[int, List[Dict]]:
        cursor = self._connection.cursor()
        if guild_id:
            cursor.execute('SELECT * FROM warnings WHERE guild_id = ? AND is_active = 1', (guild_id,))
        else:
            cursor.execute('SELECT * FROM warnings WHERE is_active = 1')

        result: Dict[int, List[Dict]] = {}
        for row in cursor.fetchall():
            user_id = row['user_id']
            if user_id not in result:
                result[user_id] = []
            result[user_id].append({
                'warning_id': row['warning_id'],
                'user_id': row['user_id'],
                'guild_id': row['guild_id'],
                'moderator_id': row['moderator_id'],
                'warning_type': row['warning_type'],
                'reason': row['reason'],
                'points': row['points'],
                'created_at': row['created_at'],
                'expires_at': row['expires_at'],
                'is_active': bool(row['is_active'])
            })
        return result

    def delete_warning(self, warning_id: str) -> bool:
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute('UPDATE warnings SET is_active = 0 WHERE warning_id = ?', (warning_id,))
            self._connection.commit()
            return cursor.rowcount > 0

    # =========================================================================
    # TICKET TOOL METHODS
    # =========================================================================

    # === TICKET PANELS ===
    def save_ticket_panel(self, panel: Dict) -> None:
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO ticket_panels
                (panel_id, guild_id, channel_id, message_id, name, description, embed_title, 
                 embed_description, embed_color, embed_image, embed_thumbnail, button_style, 
                 button_label, button_emoji, category_id, support_role_id, ticket_limit, 
                 auto_close_hours, welcome_message, claim_required, created_at, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                panel.get('panel_id'),
                panel.get('guild_id'),
                panel.get('channel_id'),
                panel.get('message_id'),
                panel.get('name'),
                panel.get('description'),
                panel.get('embed_title'),
                panel.get('embed_description'),
                panel.get('embed_color', 0x5865F2),
                panel.get('embed_image'),
                panel.get('embed_thumbnail'),
                panel.get('button_style', 3),
                panel.get('button_label', 'Create Ticket'),
                panel.get('button_emoji'),
                panel.get('category_id'),
                panel.get('support_role_id'),
                panel.get('ticket_limit', 1),
                panel.get('auto_close_hours', 24),
                panel.get('welcome_message'),
                panel.get('claim_required', 0),
                panel.get('created_at'),
                panel.get('is_active', 1)
            ))
            self._connection.commit()

    def load_ticket_panel(self, panel_id: str) -> Optional[Dict]:
        cursor = self._connection.cursor()
        cursor.execute('SELECT * FROM ticket_panels WHERE panel_id = ?', (panel_id,))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None

    def load_ticket_panels_by_guild(self, guild_id: int) -> List[Dict]:
        cursor = self._connection.cursor()
        cursor.execute('SELECT * FROM ticket_panels WHERE guild_id = ? AND is_active = 1', (guild_id,))
        return [dict(row) for row in cursor.fetchall()]

    def load_ticket_panel_by_message(self, message_id: int) -> Optional[Dict]:
        cursor = self._connection.cursor()
        cursor.execute('SELECT * FROM ticket_panels WHERE message_id = ?', (message_id,))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None

    def delete_ticket_panel(self, panel_id: str) -> bool:
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute('UPDATE ticket_panels SET is_active = 0 WHERE panel_id = ?', (panel_id,))
            self._connection.commit()
            return cursor.rowcount > 0

    # === TICKETS ===
    def save_ticket(self, ticket: Dict) -> None:
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO tickets
                (ticket_id, guild_id, channel_id, panel_id, creator_id, category, subject, 
                 claimed_by, claimed_at, status, created_at, closed_at, closed_by, 
                 close_reason, rating, rating_feedback)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                ticket.get('ticket_id'),
                ticket.get('guild_id'),
                ticket.get('channel_id'),
                ticket.get('panel_id'),
                ticket.get('creator_id'),
                ticket.get('category'),
                ticket.get('subject'),
                ticket.get('claimed_by'),
                ticket.get('claimed_at'),
                ticket.get('status', 'open'),
                ticket.get('created_at'),
                ticket.get('closed_at'),
                ticket.get('closed_by'),
                ticket.get('close_reason'),
                ticket.get('rating'),
                ticket.get('rating_feedback')
            ))
            self._connection.commit()

    def load_ticket(self, ticket_id: str) -> Optional[Dict]:
        cursor = self._connection.cursor()
        cursor.execute('SELECT * FROM tickets WHERE ticket_id = ?', (ticket_id,))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None

    def load_ticket_by_channel(self, channel_id: int) -> Optional[Dict]:
        cursor = self._connection.cursor()
        cursor.execute('SELECT * FROM tickets WHERE channel_id = ?', (channel_id,))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None

    def load_tickets_by_guild(self, guild_id: int, status: str = None) -> List[Dict]:
        cursor = self._connection.cursor()
        if status:
            cursor.execute('SELECT * FROM tickets WHERE guild_id = ? AND status = ?', (guild_id, status))
        else:
            cursor.execute('SELECT * FROM tickets WHERE guild_id = ?', (guild_id,))
        return [dict(row) for row in cursor.fetchall()]

    def load_tickets_by_creator(self, creator_id: int, guild_id: int = None) -> List[Dict]:
        cursor = self._connection.cursor()
        if guild_id:
            cursor.execute('SELECT * FROM tickets WHERE creator_id = ? AND guild_id = ? AND status = "open"', (creator_id, guild_id))
        else:
            cursor.execute('SELECT * FROM tickets WHERE creator_id = ? AND status = "open"', (creator_id,))
        return [dict(row) for row in cursor.fetchall()]

    def load_tickets_by_claimed(self, claimed_by: int, guild_id: int = None) -> List[Dict]:
        cursor = self._connection.cursor()
        if guild_id:
            cursor.execute('SELECT * FROM tickets WHERE claimed_by = ? AND guild_id = ? AND status = "open"', (claimed_by, guild_id))
        else:
            cursor.execute('SELECT * FROM tickets WHERE claimed_by = ? AND status = "open"', (claimed_by,))
        return [dict(row) for row in cursor.fetchall()]

    def load_all_open_tickets(self) -> List[Dict]:
        cursor = self._connection.cursor()
        cursor.execute('SELECT * FROM tickets WHERE status = "open"')
        return [dict(row) for row in cursor.fetchall()]

    # === TICKET TRANSCRIPTS ===
    def save_transcript(self, transcript: Dict) -> None:
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute('''
                INSERT INTO ticket_transcripts
                (transcript_id, ticket_id, guild_id, channel_id, creator_id, closed_by, 
                 claimed_by, category, created_at, closed_at, message_count, file_path, html_content)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                transcript.get('transcript_id'),
                transcript.get('ticket_id'),
                transcript.get('guild_id'),
                transcript.get('channel_id'),
                transcript.get('creator_id'),
                transcript.get('closed_by'),
                transcript.get('claimed_by'),
                transcript.get('category'),
                transcript.get('created_at'),
                transcript.get('closed_at'),
                transcript.get('message_count', 0),
                transcript.get('file_path'),
                transcript.get('html_content')
            ))
            self._connection.commit()

    def load_transcripts_by_guild(self, guild_id: int, limit: int = 50) -> List[Dict]:
        cursor = self._connection.cursor()
        cursor.execute('SELECT * FROM ticket_transcripts WHERE guild_id = ? ORDER BY closed_at DESC LIMIT ?', (guild_id, limit))
        return [dict(row) for row in cursor.fetchall()]

    def load_transcript(self, ticket_id: str) -> Optional[Dict]:
        cursor = self._connection.cursor()
        cursor.execute('SELECT * FROM ticket_transcripts WHERE ticket_id = ?', (ticket_id,))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None

    # === TICKET BLACKLIST ===
    def save_ticket_blacklist(self, blacklist: Dict) -> None:
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO ticket_blacklist
                (blacklist_id, guild_id, user_id, reason, blacklisted_by, blacklisted_at, expires_at, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                blacklist.get('blacklist_id'),
                blacklist.get('guild_id'),
                blacklist.get('user_id'),
                blacklist.get('reason'),
                blacklist.get('blacklisted_by'),
                blacklist.get('blacklisted_at'),
                blacklist.get('expires_at'),
                blacklist.get('is_active', 1)
            ))
            self._connection.commit()

    def is_user_blacklisted(self, guild_id: int, user_id: int) -> Tuple[bool, Optional[str]]:
        cursor = self._connection.cursor()
        cursor.execute('''
            SELECT * FROM ticket_blacklist 
            WHERE guild_id = ? AND user_id = ? AND is_active = 1
        ''', (guild_id, user_id))
        row = cursor.fetchone()
        if row:
            # Check if expired
            if row['expires_at']:
                expires = datetime.fromisoformat(row['expires_at'])
                if datetime.now(timezone.utc) > expires:
                    return False, None
            return True, row['reason']
        return False, None

    def load_ticket_blacklist(self, guild_id: int) -> List[Dict]:
        cursor = self._connection.cursor()
        cursor.execute('SELECT * FROM ticket_blacklist WHERE guild_id = ? AND is_active = 1', (guild_id,))
        return [dict(row) for row in cursor.fetchall()]

    def remove_ticket_blacklist(self, guild_id: int, user_id: int) -> bool:
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute('UPDATE ticket_blacklist SET is_active = 0 WHERE guild_id = ? AND user_id = ?', (guild_id, user_id))
            self._connection.commit()
            return cursor.rowcount > 0

    # === TICKET SETTINGS ===
    def save_ticket_settings(self, settings: Dict) -> None:
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO ticket_settings
                (guild_id, category_id, transcripts_channel_id, log_channel_id, support_role_id, 
                 admin_role_id, max_tickets_per_user, auto_close_hours, mention_on_create, 
                 dm_transcripts, require_claim, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                settings.get('guild_id'),
                settings.get('category_id'),
                settings.get('transcripts_channel_id'),
                settings.get('log_channel_id'),
                settings.get('support_role_id'),
                settings.get('admin_role_id'),
                settings.get('max_tickets_per_user', 3),
                settings.get('auto_close_hours', 24),
                settings.get('mention_on_create', 1),
                settings.get('dm_transcripts', 1),
                settings.get('require_claim', 0),
                settings.get('created_at'),
                settings.get('updated_at')
            ))
            self._connection.commit()

    def load_ticket_settings(self, guild_id: int) -> Optional[Dict]:
        cursor = self._connection.cursor()
        cursor.execute('SELECT * FROM ticket_settings WHERE guild_id = ?', (guild_id,))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None

    # === TICKET QUESTIONS ===
    def save_ticket_question(self, question: Dict) -> None:
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO ticket_questions
                (question_id, panel_id, guild_id, question_text, question_type, required, placeholder, order_index, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                question.get('question_id'),
                question.get('panel_id'),
                question.get('guild_id'),
                question.get('question_text'),
                question.get('question_type', 'text'),
                question.get('required', 1),
                question.get('placeholder'),
                question.get('order_index', 0),
                question.get('created_at')
            ))
            self._connection.commit()

    def load_panel_questions(self, panel_id: str) -> List[Dict]:
        cursor = self._connection.cursor()
        cursor.execute('SELECT * FROM ticket_questions WHERE panel_id = ? ORDER BY order_index', (panel_id,))
        return [dict(row) for row in cursor.fetchall()]

    def delete_ticket_question(self, question_id: str) -> bool:
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute('DELETE FROM ticket_questions WHERE question_id = ?', (question_id,))
            self._connection.commit()
            return cursor.rowcount > 0

    # === TICKET ANSWERS ===
    def save_ticket_answer(self, answer: Dict) -> None:
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute('''
                INSERT INTO ticket_answers (answer_id, ticket_id, question_id, user_id, answer_text, answered_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                answer.get('answer_id'),
                answer.get('ticket_id'),
                answer.get('question_id'),
                answer.get('user_id'),
                answer.get('answer_text'),
                answer.get('answered_at')
            ))
            self._connection.commit()

    def load_ticket_answers(self, ticket_id: str) -> List[Dict]:
        cursor = self._connection.cursor()
        cursor.execute('SELECT * FROM ticket_answers WHERE ticket_id = ?', (ticket_id,))
        return [dict(row) for row in cursor.fetchall()]

    # === TICKET MESSAGES ===
    def save_ticket_message(self, message: Dict) -> None:
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO ticket_messages
                (message_id, ticket_id, author_id, author_name, author_avatar, content, attachments, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                message.get('message_id'),
                message.get('ticket_id'),
                message.get('author_id'),
                message.get('author_name'),
                message.get('author_avatar'),
                message.get('content'),
                message.get('attachments'),
                message.get('created_at')
            ))
            self._connection.commit()

    def load_ticket_messages(self, ticket_id: str) -> List[Dict]:
        cursor = self._connection.cursor()
        cursor.execute('SELECT * FROM ticket_messages WHERE ticket_id = ? ORDER BY created_at', (ticket_id,))
        return [dict(row) for row in cursor.fetchall()]

    def delete_ticket_messages(self, ticket_id: str) -> None:
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute('DELETE FROM ticket_messages WHERE ticket_id = ?', (ticket_id,))
            self._connection.commit()

    # === TICKET NOTES ===
    def save_ticket_note(self, note: Dict) -> None:
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO ticket_notes
                (note_id, ticket_id, guild_id, author_id, content, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                note.get('note_id'),
                note.get('ticket_id'),
                note.get('guild_id'),
                note.get('author_id'),
                note.get('content'),
                note.get('created_at'),
            ))
            self._connection.commit()

    def load_ticket_notes(self, ticket_id: str) -> List[Dict]:
        cursor = self._connection.cursor()
        cursor.execute('SELECT * FROM ticket_notes WHERE ticket_id = ? ORDER BY created_at', (ticket_id,))
        return [dict(row) for row in cursor.fetchall()]

    def delete_ticket_note(self, note_id: str) -> bool:
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute('DELETE FROM ticket_notes WHERE note_id = ?', (note_id,))
            self._connection.commit()
            return cursor.rowcount > 0

    # === TICKET STATISTICS ===
    def load_ticket_stats(self, guild_id: int) -> Dict:
        cursor = self._connection.cursor()
        cursor.execute('SELECT COUNT(*) as total FROM tickets WHERE guild_id = ?', (guild_id,))
        total = cursor.fetchone()['total']

        cursor.execute('SELECT COUNT(*) as total FROM tickets WHERE guild_id = ? AND status = "open"', (guild_id,))
        open_count = cursor.fetchone()['total']

        cursor.execute('SELECT COUNT(*) as total FROM tickets WHERE guild_id = ? AND status = "closed"', (guild_id,))
        closed_count = cursor.fetchone()['total']

        cursor.execute('''
            SELECT AVG(rating) as avg_rating FROM tickets
            WHERE guild_id = ? AND rating IS NOT NULL
        ''', (guild_id,))
        row = cursor.fetchone()
        avg_rating = round(row['avg_rating'], 2) if row['avg_rating'] else None

        # Average close time in hours
        cursor.execute('''
            SELECT created_at, closed_at FROM tickets
            WHERE guild_id = ? AND status = "closed" AND closed_at IS NOT NULL
        ''', (guild_id,))
        rows = cursor.fetchall()
        avg_hours = None
        if rows:
            durations = []
            for r in rows:
                try:
                    created = datetime.fromisoformat(r['created_at'].replace('Z', '+00:00'))
                    closed = datetime.fromisoformat(r['closed_at'].replace('Z', '+00:00'))
                    durations.append((closed - created).total_seconds() / 3600)
                except Exception:
                    pass
            if durations:
                avg_hours = round(sum(durations) / len(durations), 2)

        # Priority breakdown
        cursor.execute('''
            SELECT priority, COUNT(*) as cnt FROM tickets
            WHERE guild_id = ? AND status = "open"
            GROUP BY priority
        ''', (guild_id,))
        priority_rows = cursor.fetchall()
        priorities = {r['priority']: r['cnt'] for r in priority_rows}

        return {
            'total': total,
            'open': open_count,
            'closed': closed_count,
            'avg_rating': avg_rating,
            'avg_close_hours': avg_hours,
            'priorities': priorities,
        }

    def update_ticket_first_response(self, ticket_id: str) -> None:
        """Record first staff response time if not already set."""
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute(
                'UPDATE tickets SET first_response_at = ? WHERE ticket_id = ? AND first_response_at IS NULL',
                (datetime.now(timezone.utc).isoformat(), ticket_id)
            )
            self._connection.commit()

    def purge_stale_data(
        self,
        valid_ticket_ids: set,
        valid_panel_ids: set,
        orphaned_open_ticket_ids: set,
        expired_blacklist_ids: set,
        closed_ticket_ids: set,
    ) -> Dict[str, int]:
        """
        Delete rows that are no longer referenced, valid, or needed.
        Returns a dict of table -> rows deleted for the report.
        """
        counts: Dict[str, int] = {}

        with self._lock:
            cursor = self._connection.cursor()

            # 1. Delete all closed tickets and cascade all their child data
            if closed_ticket_ids:
                ph = ','.join('?' * len(closed_ticket_ids))
                ids = list(closed_ticket_ids)

                cursor.execute(f'DELETE FROM ticket_answers    WHERE ticket_id IN ({ph})', ids)
                counts['closed_answers'] = cursor.rowcount

                cursor.execute(f'DELETE FROM ticket_notes      WHERE ticket_id IN ({ph})', ids)
                counts['closed_notes'] = cursor.rowcount

                cursor.execute(f'DELETE FROM ticket_messages   WHERE ticket_id IN ({ph})', ids)
                counts['closed_messages'] = cursor.rowcount

                cursor.execute(f'DELETE FROM ticket_transcripts WHERE ticket_id IN ({ph})', ids)
                counts['closed_transcripts'] = cursor.rowcount

                cursor.execute(f'DELETE FROM tickets           WHERE ticket_id IN ({ph})', ids)
                counts['closed_tickets'] = cursor.rowcount

                # Rebuild valid_ticket_ids after deleting closed ones
                valid_ticket_ids = valid_ticket_ids - closed_ticket_ids
            else:
                counts['closed_tickets'] = 0
                counts['closed_answers'] = 0
                counts['closed_notes'] = 0
                counts['closed_messages'] = 0
                counts['closed_transcripts'] = 0

            # 2. Orphaned child rows for any remaining invalid ticket IDs
            if valid_ticket_ids:
                ph2 = ','.join('?' * len(valid_ticket_ids))
                ids2 = list(valid_ticket_ids)

                cursor.execute(f'DELETE FROM ticket_answers     WHERE ticket_id NOT IN ({ph2})', ids2)
                counts['orphan_answers'] = cursor.rowcount

                cursor.execute(f'DELETE FROM ticket_notes       WHERE ticket_id NOT IN ({ph2})', ids2)
                counts['orphan_notes'] = cursor.rowcount

                cursor.execute(f'DELETE FROM ticket_messages    WHERE ticket_id NOT IN ({ph2})', ids2)
                counts['orphan_messages'] = cursor.rowcount

                cursor.execute(f'DELETE FROM ticket_transcripts  WHERE ticket_id NOT IN ({ph2})', ids2)
                counts['orphan_transcripts'] = cursor.rowcount
            else:
                # No open tickets remain — wipe all remaining child rows
                for tbl in ('ticket_answers', 'ticket_notes', 'ticket_messages', 'ticket_transcripts'):
                    cursor.execute(f'DELETE FROM {tbl}')
                    counts[f'orphan_{tbl.replace("ticket_", "")}'] = cursor.rowcount

            # 3. Orphaned questions for deleted panels
            if valid_panel_ids:
                ph3 = ','.join('?' * len(valid_panel_ids))
                cursor.execute(
                    f'DELETE FROM ticket_questions WHERE panel_id NOT IN ({ph3})',
                    list(valid_panel_ids)
                )
            else:
                cursor.execute('DELETE FROM ticket_questions')
            counts['orphan_questions'] = cursor.rowcount

            # 4. Mark open tickets as closed where the Discord channel is gone
            if orphaned_open_ticket_ids:
                ph4 = ','.join('?' * len(orphaned_open_ticket_ids))
                cursor.execute(
                    f'''UPDATE tickets SET status = "closed",
                        close_reason = "Channel deleted - cleaned by dbcleanup",
                        closed_at = ?
                        WHERE ticket_id IN ({ph4})''',
                    [datetime.now(timezone.utc).isoformat()] + list(orphaned_open_ticket_ids)
                )
                counts['orphaned_tickets_closed'] = cursor.rowcount
            else:
                counts['orphaned_tickets_closed'] = 0

            # 5. Remove expired / inactive blacklist entries
            if expired_blacklist_ids:
                ph5 = ','.join('?' * len(expired_blacklist_ids))
                cursor.execute(
                    f'DELETE FROM ticket_blacklist WHERE blacklist_id IN ({ph5})',
                    list(expired_blacklist_ids)
                )
                counts['ticket_blacklist'] = cursor.rowcount
            else:
                counts['ticket_blacklist'] = 0

            # 6. Remove deactivated warnings
            cursor.execute('DELETE FROM warnings WHERE is_active = 0')
            counts['warnings'] = cursor.rowcount

            self._connection.commit()

        return counts

    def load_all_tickets(self) -> List[Dict]:
        cursor = self._connection.cursor()
        cursor.execute('SELECT * FROM tickets')
        return [dict(row) for row in cursor.fetchall()]

    def load_all_ticket_panels(self) -> List[Dict]:
        cursor = self._connection.cursor()
        cursor.execute('SELECT * FROM ticket_panels')
        return [dict(row) for row in cursor.fetchall()]

    def load_all_ticket_blacklist(self) -> List[Dict]:
        cursor = self._connection.cursor()
        cursor.execute('SELECT * FROM ticket_blacklist')
        return [dict(row) for row in cursor.fetchall()]


# --- DATA MODELS ---
@dataclass
class Warning:
    warning_id: str = ""
    user_id: int = 0
    guild_id: int = 0
    moderator_id: int = 0
    warning_type: WarningType = WarningType.CUSTOM
    reason: str = ""
    points: int = 1
    created_at: datetime = None
    expires_at: datetime = None
    is_active: bool = True
    
    def __post_init__(self):
        if not self.warning_id:
            import uuid
            self.warning_id = str(uuid.uuid4())[:8]
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)


@dataclass
class Ticket:
    ticket_id: str = ""
    channel_id: int = 0
    guild_id: int = 0
    creator_id: int = 0
    category: str = "general"
    subject: str = ""
    status: TicketStatus = TicketStatus.OPEN
    assigned_to: int = None
    created_at: datetime = None
    
    def __post_init__(self):
        if not self.ticket_id:
            import uuid
            self.ticket_id = str(uuid.uuid4())[:8]
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)


@dataclass
class Giveaway:
    giveaway_id: str = ""
    message_id: int = 0
    channel_id: int = 0
    guild_id: int = 0
    host_id: int = 0
    prize: str = ""
    winner_count: int = 1
    entries: List[int] = None
    winners: List[int] = None
    status: GiveawayStatus = GiveawayStatus.ACTIVE
    created_at: datetime = None
    ends_at: datetime = None
    
    def __post_init__(self):
        if not self.giveaway_id:
            import uuid
            self.giveaway_id = str(uuid.uuid4())[:8]
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)
        if self.entries is None:
            self.entries = []
        if self.winners is None:
            self.winners = []


@dataclass
class UserLevel:
    user_id: int = 0
    guild_id: int = 0
    xp: int = 0
    level: int = 0
    total_messages: int = 0
    last_xp_gain: datetime = None
    
    @property
    def xp_for_next_level(self) -> int:
        return int((self.level + 1) ** 2.5 * 100)
    
    @property
    def xp_progress(self) -> float:
        xp_needed = self.xp_for_next_level
        current_level_xp = int(self.level ** 2.5 * 100)
        xp_in_current_level = self.xp - current_level_xp
        xp_for_next = xp_needed - current_level_xp
        return min(100.0, (xp_in_current_level / xp_for_next) * 100) if xp_for_next > 0 else 100.0
    
    def add_xp(self, amount: int) -> bool:
        self.xp += amount
        self.total_messages += 1
        if self.xp >= self.xp_for_next_level:
            self.level += 1
            return True
        return False


# --- GLOBAL INSTANCE ---
config = Config()
config.ensure_directories()
data_manager = DataManager(config.db_file)


# --- LOGGING SETUP ---
def setup_logging() -> None:
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler = RotatingFileHandler(
        'bot.log',
        maxBytes=1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(log_formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(log_formatter)
    logging.basicConfig(level=logging.INFO, handlers=[file_handler, stream_handler])


setup_logging()


# --- BOT INTENTS AND INSTANCE ---
intents = discord.Intents.default()
intents.members = True
intents.message_content = True
intents.invites = True
intents.presences = True
intents.reactions = True

bot = commands.Bot(command_prefix=config.command_prefix, intents=intents)


# --- GLOBAL STATE ---
verification_flags: Dict[int, bool] = {}
verification_cooldowns: Dict[int, float] = {}
messages_enabled: bool = False
start_time: float = time.time()
invite_manager: Optional['InviteManager'] = None
blacklisted_keywords: Set[str] = set()
rules_cache: Dict[str, Any] = {'mos_rules': '', 'vprp_rules': '', 'mos_last_updated': None, 'vprp_last_updated': None}

# New V2 data stores
warnings_data: Dict[int, List[Dict]] = {}  # guild_id -> list of warnings
tickets_data: Dict[str, Dict] = {}  # ticket_id -> ticket data
giveaways_data: Dict[str, Dict] = {}  # giveaway_id -> giveaway data
levels_data: Dict[Tuple[int, int], Dict] = {}  # (user_id, guild_id) -> level data


# --- PROCESS MANAGER ---
class ProcessManager:
    def __init__(self):
        self._active_verifications: Set[int] = set()
        self._active_processes: int = 0
    
    def add_verification(self, user_id: int) -> None:
        self._active_verifications.add(user_id)
        self._update_lock_file()
    
    def remove_verification(self, user_id: int) -> None:
        self._active_verifications.discard(user_id)
        self._update_lock_file()
    
    def is_busy(self) -> bool:
        return len(self._active_verifications) > 0 or self._active_processes > 0
    
    def _update_lock_file(self) -> None:
        try:
            if self.is_busy():
                with open(config.lock_file, 'w') as f:
                    f.write(f"busy since: {datetime.now().isoformat()}")
            else:
                if os.path.exists(config.lock_file):
                    os.remove(config.lock_file)
        except Exception as e:
            logging.error(f"Error updating lock file: {e}")
    
    def clear_lock_file(self) -> None:
        try:
            if os.path.exists(config.lock_file):
                os.remove(config.lock_file)
        except Exception as e:
            logging.error(f"Error removing lock file: {e}")


process_manager = ProcessManager()


# --- TEMPLATES ---
VERIFICATION_QUESTIONS: List[Tuple[str, str]] = [
    ("Before we start, Do you agree to following the terms and Rules of VPRP and MOS?", "Agreement"),
    ("What is your age?", "Age"),
    ("What other Gangs are you affiliated with? (Reply 'Skip' if you're an ally)", "Gang Affiliations"),
    ("Send a screenshot of the member who invited you. (Reply 'Skip' if you're an ally)", "Invitation Proof"),
    ("How good would you say your aim is, And do you use any aim trainers in your spare time?", "Aim Skill & Training"),
    ("Do you have any Previous Experiences with Gangs [Ex: FiveM, Ro hood rp, Street shooters, Etc..]?", "Previous Gang Experience"),
    ("What is your Discord username?", "Discord Username"),
    ("What is your ROBLOX Username?", "ROBLOX Username"),
    ("ROBLOX_VERIFICATION_STEP", "Roblox Verification"),  # Special marker for verification
    ("What do you want your gang name to be? (Format: MOS_YourGangName)", "Desired Gang Name"),
    ("How was your experience with our verification system?", "Feedback")
]

INVITE_CONFIGS: List[Dict[str, Any]] = [
    {"name": "10 Uses", "max_uses": 10, "max_age": 0},
    {"name": "25 Uses", "max_uses": 25, "max_age": 0},
    {"name": "50 Uses", "max_uses": 50, "max_age": 0},
]

REPORT_TEMPLATES: List[str] = [
    "Ay, Someone pass me a blunt. Damn.. No one? Whatever, ay if someone is disrespecting "
    "members or allies type '!report <Ping the User> <Reason Here>'",
    "Ay, Yall wanna slide on the opps?",
    "Yall know it's MOS Shit. **MASK OFF SOCIETY** ON TOP!"
]

WELCOME_TEMPLATES: List[str] = [
    "Welcome {mention} to Mask Off Society, MOS Expects you to put in work, if you do to that, You'll fit in just fine.",
    "What's Good, {mention}. Brought any KFC? No? Whatever.. Welcome to MOS, we expect you to put in work.",
    "Yoooo Big Dawg {mention} dropped in yall, Wuz Gud, Dawg?",
    "Is that who I think it is? {mention} welcome to MOS, we expect you to put in work.",
    "Ay {mention} we gotta bust a cap in the opps some time soon Dawg, Let me know when you're down.",
    "Well, Well, Well, If it isnt the one and only thug, {mention}. We're glad to have you dawg, MOS Expects you to put in work."
]

REPORT_CATEGORIES: Dict[str, str] = {
    "1ï¸âƒ£": "Rule Violation",
    "2ï¸âƒ£": "Harassment",
    "3ï¸âƒ£": "Scamming",
    "4ï¸âƒ£": "Cheating",
    "5ï¸âƒ£": "Other"
}

TICKET_CATEGORIES: Dict[str, str] = {
    "general": "General Support",
    "report": "Player Report",
    "appeal": "Ban Appeal",
    "verification": "Verification Help",
    "other": "Other",
    "alliance": "Request an Alliance",
    "opp": "Request us to Add opp gangs / players"
}

POLL_EMOJIS: List[str] = ["1ï¸âƒ£", "2ï¸âƒ£", "3ï¸âƒ£", "4ï¸âƒ£", "5ï¸âƒ£", "6ï¸âƒ£", "7ï¸âƒ£", "8ï¸âƒ£", "9ï¸âƒ£", "ðŸ”Ÿ"]


# --- EMBED BUILDER (V2 Enhancement) ---
class EmbedBuilder:
    @staticmethod
    def success(title: str, description: str) -> discord.Embed:
        return discord.Embed(title=title, description=description, color=discord.Color.green(), timestamp=datetime.now(timezone.utc))
    
    @staticmethod
    def error(title: str, description: str) -> discord.Embed:
        return discord.Embed(title=title, description=description, color=discord.Color.red(), timestamp=datetime.now(timezone.utc))
    
    @staticmethod
    def warning(title: str, description: str) -> discord.Embed:
        return discord.Embed(title=title, description=description, color=discord.Color.orange(), timestamp=datetime.now(timezone.utc))
    
    @staticmethod
    def info(title: str, description: str) -> discord.Embed:
        return discord.Embed(title=title, description=description, color=discord.Color.blue(), timestamp=datetime.now(timezone.utc))
    
    @staticmethod
    def verification(title: str, description: str) -> discord.Embed:
        return discord.Embed(title=title, description=description, color=discord.Color.gold(), timestamp=datetime.now(timezone.utc))
    
    @staticmethod
    def ticket(title: str, description: str) -> discord.Embed:
        return discord.Embed(title=title, description=description, color=discord.Color.blurple(), timestamp=datetime.now(timezone.utc))
    
    @staticmethod
    def giveaway(title: str, description: str) -> discord.Embed:
        return discord.Embed(title=title, description=description, color=discord.Color.purple(), timestamp=datetime.now(timezone.utc))
    
    @staticmethod
    def level(user: discord.Member, level_data: Dict) -> discord.Embed:
        embed = discord.Embed(title=f"ðŸ“Š Level Card: {user.display_name}", color=discord.Color.gold())
        if user.avatar:
            embed.set_thumbnail(url=user.avatar.url)
        
        level = level_data.get('level', 0)
        xp = level_data.get('xp', 0)
        total_messages = level_data.get('total_messages', 0)
        
        xp_for_next = int((level + 1) ** 2.5 * 100)
        current_xp = int(level ** 2.5 * 100)
        progress = min(100.0, ((xp - current_xp) / (xp_for_next - current_xp)) * 100) if xp_for_next > current_xp else 100.0
        
        progress_bar = "â–ˆ" * int(progress / 10) + "â–‘" * (10 - int(progress / 10))
        
        embed.add_field(name="Level", value=f"**{level}**", inline=True)
        embed.add_field(name="XP", value=f"{xp:,}", inline=True)
        embed.add_field(name="Progress", value=f"`{progress_bar}` {progress:.1f}%", inline=True)
        embed.add_field(name="Messages", value=f"{total_messages:,}", inline=True)
        
        return embed


# --- PAGINATED VIEW (V2 Enhancement) ---
class PaginatedView(View):
    def __init__(self, user_id: int, items: List[str], title: str, color: discord.Color, items_per_page: int = 5, timeout: float = 180):
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.items = items
        self.title = title
        self.color = color
        self.items_per_page = items_per_page
        self.current_page = 0
        self.total_pages = max(1, (len(items) + items_per_page - 1) // items_per_page)
        self._update_buttons()
    
    def _update_buttons(self) -> None:
        self.previous_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page >= self.total_pages - 1
    
    def _create_embed(self) -> discord.Embed:
        start = self.current_page * self.items_per_page
        end = start + self.items_per_page
        page_items = self.items[start:end]
        
        description = "\n\n".join(str(item) for item in page_items)
        
        embed = discord.Embed(title=self.title, description=description or "No items to display.", color=self.color)
        embed.set_footer(text=f"Page {self.current_page + 1}/{self.total_pages}")
        embed.timestamp = datetime.now(timezone.utc)
        
        return embed
    
    @discord.ui.button(label="â—€ï¸ Previous", style=discord.ButtonStyle.secondary)
    async def previous_button(self, interaction: discord.Interaction, button: Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This isn't your menu!", ephemeral=True)
            return
        if self.current_page > 0:
            self.current_page -= 1
            self._update_buttons()
            await interaction.response.edit_message(embed=self._create_embed(), view=self)
        else:
            await interaction.response.defer()
    
    @discord.ui.button(label="â–¶ï¸ Next", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, button: Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This isn't your menu!", ephemeral=True)
            return
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            self._update_buttons()
            await interaction.response.edit_message(embed=self._create_embed(), view=self)
        else:
            await interaction.response.defer()


# --- RULES VIEWS ---
class PaginatedRulesView(View):
    def __init__(self, user_id: int, title: str, emoji: str, color: discord.Color, rules_pages: List[str], timeout: float = 180):
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.title = title
        self.emoji = emoji
        self.color = color
        self.rules_pages = rules_pages
        self.current_page = 0
        self._update_buttons()
    
    def _update_buttons(self) -> None:
        self.previous_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page >= len(self.rules_pages) - 1
    
    def _create_embed(self) -> discord.Embed:
        embed = discord.Embed(title=f"{self.emoji} {self.title}", description=self.rules_pages[self.current_page], color=self.color)
        embed.set_footer(text=f"Page {self.current_page + 1}/{len(self.rules_pages)}  MOS Verification System")
        embed.timestamp = datetime.now(timezone.utc)
        return embed
    
    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary, custom_id="rules_prev_page")
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This isn't your verification!", ephemeral=True)
            return
        if self.current_page > 0:
            self.current_page -= 1
            self._update_buttons()
            await interaction.response.edit_message(embed=self._create_embed(), view=self)
        else:
            await interaction.response.defer()
    
    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, custom_id="rules_next_page")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This isn't your verification!", ephemeral=True)
            return
        if self.current_page < len(self.rules_pages) - 1:
            self.current_page += 1
            self._update_buttons()
            await interaction.response.edit_message(embed=self._create_embed(), view=self)
        else:
            await interaction.response.defer()


def split_rules_into_pages(rules: str, max_chars: int = 3900) -> List[str]:
    if not rules:
        return ["No rules available. Contact staff."]
    if len(rules) <= max_chars:
        return [rules]
    
    pages: List[str] = []
    current_page = ""
    sections = rules.split('\n---\n')
    
    for section in sections:
        if len(section) > max_chars:
            paragraphs = section.split('\n\n')
            for para in paragraphs:
                if len(current_page) + len(para) + 2 > max_chars:
                    if current_page:
                        pages.append(current_page.strip())
                    current_page = para + "\n\n"
                else:
                    current_page += para + "\n\n"
        else:
            if len(current_page) + len(section) + 10 > max_chars:
                if current_page:
                    pages.append(current_page.strip())
                current_page = section + "\n\n---\n\n"
            else:
                current_page += section + "\n\n---\n\n"
    
    if current_page.strip():
        pages.append(current_page.strip())
    
    return pages if pages else [rules[:max_chars]]


class RulesButtonView(View):
    def __init__(self, user_id: int, timeout: float = 300):
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.rules_viewed = {'mos': False, 'vprp': False}
    
    def _create_rules_embed(self, title: str, rules: str, emoji: str, color: discord.Color, page_num: int = 1, total_pages: int = 1) -> discord.Embed:
        embed = discord.Embed(title=title, description=rules[:4096] if rules else "No rules available. Contact staff.", color=color)
        if total_pages > 1:
            embed.set_footer(text=f"Page {page_num}/{total_pages}  MOS Verification System")
        else:
            embed.set_footer(text="MOS Verification System")
        embed.timestamp = datetime.now(timezone.utc)
        return embed
    
    @discord.ui.button(label="MOS Rules", style=discord.ButtonStyle.primary, custom_id="mos_rules_button")
    async def mos_rules_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This isn't your verification!", ephemeral=True)
            return
        
        await interaction.response.defer(thinking=True)
        rules = await self._fetch_mos_rules()
        pages = split_rules_into_pages(rules)
        
        button.style = discord.ButtonStyle.success
        button.label = "MOS Rules Viewed"
        self.rules_viewed['mos'] = True
        await interaction.message.edit(view=self)
        
        if len(pages) > 1:
            paginated_view = PaginatedRulesView(self.user_id, "MOS In-Game Rules", "", discord.Color.red(), pages)
            await interaction.followup.send(embed=paginated_view._create_embed(), view=paginated_view, ephemeral=True)
        else:
            embed = self._create_rules_embed("MOS In-Game Rules", rules, "", discord.Color.red())
            await interaction.followup.send(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="VPRP Rules", style=discord.ButtonStyle.secondary, custom_id="vprp_rules_button")
    async def vprp_rules_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This isn't your verification!", ephemeral=True)
            return
        
        await interaction.response.defer(thinking=True)
        rules = await self._fetch_vprp_rules()
        pages = split_rules_into_pages(rules)
        
        button.style = discord.ButtonStyle.success
        button.label = "VPRP Rules Viewed"
        self.rules_viewed['vprp'] = True
        await interaction.message.edit(view=self)
        
        if len(pages) > 1:
            paginated_view = PaginatedRulesView(self.user_id, "VPRP Game & Server Rules", "", discord.Color.blue(), pages)
            await interaction.followup.send(embed=paginated_view._create_embed(), view=paginated_view, ephemeral=True)
        else:
            embed = self._create_rules_embed("VPRP Game & Server Rule", rules, "", discord.Color.blue())
            await interaction.followup.send(embed=embed, ephemeral=True)
    
    async def _fetch_mos_rules(self) -> str:
        try:
            channel = bot.get_channel(config.servers.mos_rules_channel)
            if channel:
                try:
                    message = await channel.fetch_message(config.servers.mos_rules_message)
                    rules = message.content
                    rules_cache['mos_rules'] = rules
                    rules_cache['mos_last_updated'] = datetime.now(timezone.utc).isoformat()
                    save_rules_cache()
                    return rules
                except discord.NotFound:
                    logging.warning("[Rules] MOS rules message not found, using cache")
                except discord.Forbidden:
                    logging.warning("[Rules] No permission to fetch MOS rules, using cache")
        except Exception as e:
            logging.error(f"[Rules] Error fetching MOS rules: {e}")
        
        if rules_cache['mos_rules']:
            return rules_cache['mos_rules']
        return "Unable to fetch rules. Please contact a staff member."
    
    async def _fetch_vprp_rules(self) -> str:
        try:
            vprp_guild = bot.get_guild(config.servers.vprp_server_id)
            if vprp_guild:
                channel = vprp_guild.get_channel(config.servers.vprp_rules_channel)
                if channel:
                    try:
                        message = await channel.fetch_message(config.servers.vprp_rules_message)
                        rules = message.content
                        rules_cache['vprp_rules'] = rules
                        rules_cache['vprp_last_updated'] = datetime.now(timezone.utc).isoformat()
                        save_rules_cache()
                        return rules
                    except discord.NotFound:
                        logging.warning("[Rules] VPRP rules message not found, using cache")
                    except discord.Forbidden:
                        logging.warning("[Rules] No permission to fetch VPRP rules, using cache")
            else:
                logging.warning("[Rules] Bot not in VPRP server, using cache")
        except Exception as e:
            logging.error(f"[Rules] Error fetching VPRP rules: {e}")
        
        if rules_cache['vprp_rules']:
            return rules_cache['vprp_rules']
        return "Unable to fetch VPRP rules. The bot may not be in the VPRP server.\nPlease contact a staff member or check the VPRP server directly."


# --- VERIFICATION VIEWS ---
class DeclineReasonModal(discord.ui.Modal, title="Decline Application"):
    reason_input: discord.ui.TextInput
    
    def __init__(self, user_id: int, user_name: str, original_embed: discord.Embed, original_view: 'VerificationButtonsView'):
        super().__init__()
        self.user_id = user_id
        self.user_name = user_name
        self.original_embed = original_embed
        self.original_view = original_view
        
        self.reason_input = discord.ui.TextInput(
            label="Reason for Decline",
            placeholder="Please provide a reason for declining this application...",
            style=discord.TextStyle.paragraph,
            min_length=10,
            max_length=1000,
            required=True
        )
        self.add_item(self.reason_input)
    
    async def on_submit(self, interaction: discord.Interaction) -> None:
        reason = self.reason_input.value
        try:
            user = interaction.guild.get_member(self.user_id)
            
            self.original_embed.color = discord.Color.red()
            self.original_embed.title = f"DECLINED: {self.original_embed.title.replace('Verification Submission from ', '')}"
            self.original_embed.add_field(name="Decision", value=f"**DECLINED** by {interaction.user.mention}\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", inline=False)
            self.original_embed.add_field(name="Reason", value=reason, inline=False)
            
            for child in self.original_view.children:
                child.disabled = True
            
            await interaction.message.edit(embed=self.original_embed, view=self.original_view)
            await interaction.response.send_message(f"Application for **{self.user_name}** has been declined.", ephemeral=True)
            
            await self._notify_declined_user(user, reason)
            logging.info(f"Application declined for {self.user_name} by {interaction.user} - Reason: {reason}")
        except Exception as e:
            logging.error(f"Error in decline modal submit: {str(e)}")
            await interaction.response.send_message(f"An error occurred: {str(e)}", ephemeral=True)
    
    async def _notify_declined_user(self, user: Optional[discord.Member], reason: str) -> None:
        if not user:
            return
        try:
            decline_dm = discord.Embed(title="Application Declined", description="Your MOS application has been declined.", color=discord.Color.red())
            decline_dm.add_field(name="Reason", value=reason, inline=False)
            decline_dm.add_field(name="What Now?", value="You may reapply tomorrow, Reapplying today will likely result in Mod action.", inline=False)
            decline_dm.set_footer(text="MOS Verification Team")
            await user.send(embed=decline_dm)
        except discord.Forbidden:
            welcome_channel = bot.get_channel(config.channels.welcome)
            if welcome_channel:
                await welcome_channel.send(f"{user.mention} Your application has been declined. **Reason:** {reason}\nContact staff for more information.")


class RequestMoreInfoModal(discord.ui.Modal, title="Request More Information"):
    question_input: discord.ui.TextInput
    
    def __init__(self, user_id: int, user_name: str, original_embed: discord.Embed, original_view: 'VerificationButtonsView'):
        super().__init__()
        self.user_id = user_id
        self.user_name = user_name
        self.original_embed = original_embed
        self.original_view = original_view
        
        self.question_input = discord.ui.TextInput(
            label="Question for Applicant",
            placeholder="What additional information do you need from this applicant?",
            style=discord.TextStyle.paragraph,
            min_length=10,
            max_length=1000,
            required=True
        )
        self.add_item(self.question_input)
    
    async def on_submit(self, interaction: discord.Interaction) -> None:
        question = self.question_input.value
        try:
            user = interaction.guild.get_member(self.user_id)
            if not user:
                await interaction.response.send_message("User is no longer in the server.", ephemeral=True)
                return
            
            process_manager.add_verification(self.user_id)
            
            self.original_embed.add_field(name="Info Requested", value=f"**Question:** {question}\n**By:** {interaction.user.mention}\nWaiting for response...", inline=False)
            self.original_embed.color = discord.Color.orange()
            
            for child in self.original_view.children:
                child.disabled = True
            
            await interaction.message.edit(embed=self.original_embed, view=self.original_view)
            await interaction.response.send_message(f"Question sent to **{self.user_name}**. Waiting for their response...", ephemeral=True)
            
            response_received = await self._send_question_to_applicant(user, question, interaction)
            
            if not response_received:
                for child in self.original_view.children:
                    child.disabled = False
                self.original_embed.color = discord.Color.blurple()
                self.original_embed.add_field(name="No Response", value="The applicant did not respond in time.", inline=False)
                await interaction.message.edit(embed=self.original_embed, view=self.original_view)
            
            process_manager.remove_verification(self.user_id)
        except Exception as e:
            logging.error(f"Error in request more info modal: {str(e)}")
            process_manager.remove_verification(self.user_id)
            await interaction.response.send_message(f"An error occurred: {str(e)}", ephemeral=True)
    
    async def _send_question_to_applicant(self, user: discord.Member, question: str, staff_interaction: discord.Interaction) -> bool:
        try:
            dm_embed = discord.Embed(title="Additional Information Requested", description="The MOS staff needs more information about your application.", color=discord.Color.orange())
            dm_embed.add_field(name="Question", value=question, inline=False)
            dm_embed.add_field(name="Instructions", value="Please reply to this message with your answer within **5 minutes**.", inline=False)
            dm_embed.set_footer(text="MOS Verification Team")
            await user.send(embed=dm_embed)
            
            try:
                response_msg = await bot.wait_for('message', check=lambda m: m.author.id == self.user_id and m.channel.type == discord.ChannelType.private, timeout=300)
                
                response_text = response_msg.content
                if response_msg.attachments:
                    response_text += f"\n\nAttachment: {response_msg.attachments[0].url}"
                
                self.original_embed.add_field(name=f"Applicant Response", value=f"**Answer:** {response_text[:1000]}", inline=False)
                self.original_embed.color = discord.Color.blurple()
                
                for i, field in enumerate(self.original_embed.fields):
                    if field.name == "Info Requested":
                        self.original_embed.set_field_at(i, name="Info Requested", value=f"**Question:** {question}\n**By:** {staff_interaction.user.mention}\nResponse received!", inline=False)
                        break
                
                for child in self.original_view.children:
                    child.disabled = False
                
                channel = bot.get_channel(config.channels.verification_submission)
                if channel:
                    try:
                        message = await channel.fetch_message(staff_interaction.message.id)
                        await message.edit(embed=self.original_embed, view=self.original_view)
                        await channel.send(f"**{user.display_name}** has responded to the info request!", delete_after=30)
                    except discord.NotFound:
                        pass
                
                confirm_embed = discord.Embed(title="Response Received", description="Thank you! Your response has been sent to the staff for review.", color=discord.Color.green())
                await user.send(embed=confirm_embed)
                return True
                
            except asyncio.TimeoutError:
                timeout_embed = discord.Embed(title="Time Expired", description="You did not respond in time. Please contact staff directly.", color=discord.Color.red())
                try:
                    await user.send(embed=timeout_embed)
                except discord.Forbidden:
                    pass
                return False
                
        except discord.Forbidden:
            await staff_interaction.followup.send(f"Could not DM {user.mention}. They may have DMs disabled.", ephemeral=True)
            return False
        except Exception as e:
            logging.error(f"Error sending question to applicant: {str(e)}")
            return False


class VerificationButtonsView(View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="Accept Application", style=discord.ButtonStyle.success, custom_id="accept_button")
    async def accept_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        try:
            embed = interaction.message.embeds[0]
            footer_text = embed.footer.text
            
            if "User ID:" not in footer_text:
                await interaction.response.send_message("Could not find user ID in the application.", ephemeral=True)
                return
            
            user_id = int(footer_text.split("User ID: ")[1].split(" ")[0])
            user = interaction.guild.get_member(user_id)
            
            if not user:
                await interaction.response.send_message("User not found in the server.", ephemeral=True)
                return
            
            member_role = interaction.guild.get_role(config.roles.member)
            
            if member_role:
                await user.add_roles(member_role)
                embed.color = discord.Color.green()
                embed.title = f"ACCEPTED: {embed.title.replace('Verification Submission from ', '')}"
                embed.add_field(name="Decision", value=f"**ACCEPTED** by {interaction.user.mention}\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", inline=False)
                
                for child in self.children:
                    child.disabled = True
                
                await interaction.message.edit(embed=embed, view=self)

                # --- SET NICKNAME: MOS_GangName (@Roblox_Username) ---
                nickname_set = False
                try:
                    gang_name = None
                    roblox_username = None

                    for field in embed.fields:
                        if field.name == "Desired Gang Name":
                            gang_name = field.value.strip()
                        elif field.name == "Roblox Verification" and field.value.startswith("✅ Verified:"):
                            # Value is like "✅ Verified: MOSLeader_Cash"
                            roblox_username = field.value.replace("✅ Verified:", "").strip()

                    if gang_name and roblox_username:
                        new_nickname = f"{gang_name} (@{roblox_username})"
                        # Discord nickname limit is 32 characters
                        if len(new_nickname) > 32:
                            new_nickname = new_nickname[:32]
                        await user.edit(nick=new_nickname)
                        nickname_set = True
                        logging.info(f"Nickname set to '{new_nickname}' for {user}")
                    else:
                        logging.warning(f"Could not set nickname for {user}: gang_name={gang_name}, roblox_username={roblox_username}")
                except discord.Forbidden:
                    logging.warning(f"Missing permissions to change nickname for {user}")
                except Exception as nick_err:
                    logging.error(f"Failed to set nickname for {user}: {nick_err}")
                # ----------------------------------------------------------

                nick_note = f"\nNickname set to **{new_nickname}**" if nickname_set else "\n⚠️ Could not auto-set nickname (missing permissions or data)."
                await interaction.response.send_message(
                    f"Application for {user.mention} has been accepted. Member role assigned.{nick_note}",
                    ephemeral=True
                )
                
                await self._notify_accepted_user(user)
                logging.info(f"Application accepted for {user} by {interaction.user}")
            else:
                await interaction.response.send_message("Could not find the member role to assign.", ephemeral=True)
        except Exception as e:
            logging.error(f"Error in accept button handler: {str(e)}")
            await interaction.response.send_message(f"An error occurred: {str(e)}", ephemeral=True)
    
    @discord.ui.button(label="Decline Application", style=discord.ButtonStyle.danger, custom_id="decline_button")
    async def decline_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        try:
            embed = interaction.message.embeds[0]
            footer_text = embed.footer.text
            
            if "User ID:" not in footer_text:
                await interaction.response.send_message("Could not find user ID in the application.", ephemeral=True)
                return
            
            user_id = int(footer_text.split("User ID: ")[1].split(" ")[0])
            user_name = embed.title.replace("Verification Submission from ", "")
            
            modal = DeclineReasonModal(user_id, user_name, embed, self)
            await interaction.response.send_modal(modal)
        except Exception as e:
            logging.error(f"Error in decline button handler: {str(e)}")
            await interaction.response.send_message(f"An error occurred: {str(e)}", ephemeral=True)
    
    @discord.ui.button(label="Request More Info", style=discord.ButtonStyle.secondary, custom_id="request_info_button")
    async def request_info_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        try:
            embed = interaction.message.embeds[0]
            footer_text = embed.footer.text
            
            if "User ID:" not in footer_text:
                await interaction.response.send_message("Could not find user ID in the application.", ephemeral=True)
                return
            
            user_id = int(footer_text.split("User ID: ")[1].split(" ")[0])
            user_name = embed.title.replace("Verification Submission from ", "")
            
            user = interaction.guild.get_member(user_id)
            if not user:
                await interaction.response.send_message("This user is no longer in the server.", ephemeral=True)
                return
            
            modal = RequestMoreInfoModal(user_id, user_name, embed, self)
            await interaction.response.send_modal(modal)
        except Exception as e:
            logging.error(f"Error in request info button handler: {str(e)}")
            await interaction.response.send_message(f"An error occurred: {str(e)}", ephemeral=True)
    
    async def _notify_accepted_user(self, user: discord.Member) -> None:
        try:
            accept_dm = discord.Embed(title="Application Accepted!", description="Welcome to MOS! Your application has been accepted.", color=discord.Color.green())
            accept_dm.add_field(name="What Now?", value="You now have the Member role. You're officially part of the MOS Blood.. Go on and Recruit new members, Chill with MOS Members and Allies, And just enjoy life man. Because that's what it's all about. MOS x ALLIES ON TOP!", inline=False)
            await user.send(embed=accept_dm)
        except discord.Forbidden:
            welcome_channel = bot.get_channel(config.channels.welcome)
            if welcome_channel:
                await welcome_channel.send(f"{user.mention} Your application has been accepted! Welcome to MOS!")


# --- TICKET TOOL SYSTEM (Full Ticket Tool Clone - All Features Free) ---

class TicketToolSystem:
    """Main ticket tool system manager - handles all ticket operations."""
    
    def __init__(self, data_mgr: DataManager, bot_instance: commands.Bot):
        self.data_manager = data_mgr
        self.bot = bot_instance
    
    async def create_ticket(
        self, 
        guild: discord.Guild, 
        user: discord.Member,
        panel: Dict,
        subject: str = None,
        answers: Dict = None
    ) -> Tuple[Optional[discord.TextChannel], str]:
        """Create a new ticket channel."""
        import uuid
        ticket_id = str(uuid.uuid4())[:8]
        
        # Check if user is blacklisted
        blacklisted, reason = self.data_manager.is_user_blacklisted(guild.id, user.id)
        if blacklisted:
            return None, f"You are blacklisted from creating tickets. Reason: {reason}"
        
        # Check ticket limit
        settings = self.data_manager.load_ticket_settings(guild.id)
        max_tickets = settings.get('max_tickets_per_user', 3) if settings else 3
        user_tickets = self.data_manager.load_tickets_by_creator(user.id, guild.id)
        if len(user_tickets) >= max_tickets:
            return None, f"You already have {len(user_tickets)} open ticket(s). Close one first."
        
        # Get panel settings
        category_id = panel.get('category_id') or (settings.get('category_id') if settings else None)
        support_role_id = panel.get('support_role_id') or (settings.get('support_role_id') if settings else None)
        
        # Create channel name
        channel_name = f"ticket-{user.display_name}".lower()[:50]
        channel_name = ''.join(c if c.isalnum() or c == '-' else '-' for c in channel_name)
        
        # Setup permissions
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            user: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, attach_files=True),
            guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True, read_message_history=True)
        }
        
        # Add support role
        if support_role_id:
            support_role = guild.get_role(support_role_id)
            if support_role:
                overwrites[support_role] = discord.PermissionOverwrite(
                    view_channel=True, send_messages=True, read_message_history=True, attach_files=True
                )
        
        # Get category
        category = guild.get_channel(category_id) if category_id else None
        
        try:
            channel = await guild.create_text_channel(
                channel_name,
                category=category,
                overwrites=overwrites,
                topic=f"Ticket {ticket_id} - {user}"
            )
        except Exception as e:
            logging.error(f"[TicketTool] Failed to create channel: {e}")
            return None, f"Failed to create ticket channel: {e}"
        
        # Save ticket to database
        ticket_data = {
            'ticket_id': ticket_id,
            'guild_id': guild.id,
            'channel_id': channel.id,
            'panel_id': panel.get('panel_id'),
            'creator_id': user.id,
            'category': panel.get('name', 'General'),
            'subject': subject,
            'status': 'open',
            'created_at': datetime.now(timezone.utc).isoformat()
        }
        self.data_manager.save_ticket(ticket_data)
        
        # Save answers if provided
        if answers:
            for qid, answer_text in answers.items():
                self.data_manager.save_ticket_answer({
                    'answer_id': str(uuid.uuid4())[:8],
                    'ticket_id': ticket_id,
                    'question_id': qid,
                    'user_id': user.id,
                    'answer_text': answer_text,
                    'answered_at': datetime.now(timezone.utc).isoformat()
                })
        
        return channel, ticket_id
    
    async def close_ticket(
        self,
        channel: discord.TextChannel,
        closed_by: discord.Member,
        reason: str = "No reason provided"
    ) -> bool:
        """Close a ticket and generate transcript."""
        ticket = self.data_manager.load_ticket_by_channel(channel.id)
        if not ticket:
            return False
        
        # Update ticket status
        ticket['status'] = 'closed'
        ticket['closed_at'] = datetime.now(timezone.utc).isoformat()
        ticket['closed_by'] = closed_by.id
        ticket['close_reason'] = reason
        self.data_manager.save_ticket(ticket)
        
        # Generate transcript
        transcript = await self._generate_transcript(channel, ticket, closed_by)
        
        # Send transcript to transcripts channel
        settings = self.data_manager.load_ticket_settings(channel.guild.id)
        if settings and settings.get('transcripts_channel_id'):
            transcripts_channel = channel.guild.get_channel(settings['transcripts_channel_id'])
            if transcripts_channel:
                await transcripts_channel.send(embed=transcript['embed'], file=transcript['file'])
        
        # DM transcript to user if enabled.
        # transcript['file'] is consumed after the channel send, so recreate from stored html.
        if settings and settings.get('dm_transcripts'):
            try:
                creator = channel.guild.get_member(ticket['creator_id'])
                if creator:
                    from io import BytesIO
                    dm_file = discord.File(
                        BytesIO(transcript['html'].encode('utf-8')),
                        filename=f"transcript-{ticket['ticket_id']}.html"
                    )
                    await creator.send(
                        embed=discord.Embed(
                            title=f"Ticket Closed - {channel.guild.name}",
                            description=f"Your ticket has been closed.\n**Reason:** {reason}\n\nYour transcript is attached below.",
                            color=discord.Color.orange()
                        ),
                        file=dm_file
                    )
            except Exception as e:
                logging.warning(f"[Tickets] Could not DM transcript to user: {e}")
        
        # Delete channel
        await channel.delete(reason=f"Ticket closed by {closed_by}: {reason}")
        return True
    
    async def claim_ticket(self, channel: discord.TextChannel, user: discord.Member) -> Tuple[bool, str]:
        """Claim a ticket."""
        ticket = self.data_manager.load_ticket_by_channel(channel.id)
        if not ticket:
            return False, "This is not a ticket channel."
        
        if ticket.get('claimed_by'):
            claimer = channel.guild.get_member(ticket['claimed_by'])
            return False, f"This ticket is already claimed by {claimer.mention if claimer else 'someone'}."
        
        ticket['claimed_by'] = user.id
        ticket['claimed_at'] = datetime.now(timezone.utc).isoformat()
        self.data_manager.save_ticket(ticket)
        
        return True, f"Ticket claimed by {user.mention}"
    
    async def unclaim_ticket(self, channel: discord.TextChannel, user: discord.Member) -> Tuple[bool, str]:
        """Release a ticket claim."""
        ticket = self.data_manager.load_ticket_by_channel(channel.id)
        if not ticket:
            return False, "This is not a ticket channel."
        
        if not ticket.get('claimed_by'):
            return False, "This ticket is not claimed."
        
        if ticket['claimed_by'] != user.id and not user.guild_permissions.administrator:
            return False, "You can only unclaim tickets you claimed (or be admin)."
        
        ticket['claimed_by'] = None
        ticket['claimed_at'] = None
        self.data_manager.save_ticket(ticket)
        
        return True, "Ticket unclaimed."
    
    async def _generate_transcript(self, channel: discord.TextChannel, ticket: Dict, closed_by: discord.Member) -> Dict:
        """Generate HTML transcript like Ticket Tool."""
        import uuid
        from io import BytesIO
        
        messages = []
        message_count = 0
        
        async for msg in channel.history(limit=500, oldest_first=True):
            if msg.author.bot and msg.embeds:
                continue  # Skip bot embeds
            
            message_count += 1
            messages.append({
                'author_id': msg.author.id,
                'author_name': msg.author.display_name,
                'author_avatar': str(msg.author.avatar.url) if msg.author.avatar else str(msg.author.default_avatar.url),
                'content': msg.content,
                'attachments': [att.url for att in msg.attachments],
                'timestamp': msg.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                'embeds': len(msg.embeds)
            })
        
        # Generate HTML
        html_content = self._generate_html_transcript(channel, ticket, messages, closed_by)
        
        # Save transcript to database
        transcript_id = str(uuid.uuid4())[:8]
        transcript_data = {
            'transcript_id': transcript_id,
            'ticket_id': ticket['ticket_id'],
            'guild_id': channel.guild.id,
            'channel_id': channel.id,
            'creator_id': ticket['creator_id'],
            'closed_by': closed_by.id,
            'claimed_by': ticket.get('claimed_by'),
            'category': ticket.get('category'),
            'created_at': ticket.get('created_at'),
            'closed_at': datetime.now(timezone.utc).isoformat(),
            'message_count': message_count,
            'html_content': html_content
        }
        self.data_manager.save_transcript(transcript_data)
        
        # Create embed with better formatting like Ticket Tool
        embed = discord.Embed(
            title=f"📋 Ticket Transcript - {ticket['ticket_id']}",
            description=f"**Category:** {ticket.get('category', 'General')}\n**Subject:** {ticket.get('subject', 'N/A')}",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        
        creator = channel.guild.get_member(ticket['creator_id'])
        embed.add_field(name="👤 Creator", value=creator.mention if creator else f"<@{ticket['creator_id']}>", inline=True)
        embed.add_field(name="🔒 Closed By", value=closed_by.mention, inline=True)
        embed.add_field(name="💬 Messages", value=str(message_count), inline=True)
        
        if ticket.get('claimed_by'):
            claimer = channel.guild.get_member(ticket['claimed_by'])
            embed.add_field(name="🙋 Claimed By", value=claimer.mention if claimer else f"<@{ticket['claimed_by']}>", inline=True)
        
        # Add message preview (first 5 messages) - this was missing!
        if messages:
            preview_text = ""
            for i, msg in enumerate(messages[:5]):
                content_preview = msg.get('content', '')[:100]
                if len(msg.get('content', '')) > 100:
                    content_preview += "..."
                preview_text += f"**{msg.get('author_name', 'Unknown')}:** {content_preview}\n"
            if len(messages) > 5:
                preview_text += f"\n*...and {len(messages) - 5} more messages*"
            embed.add_field(name="📝 Message Preview", value=preview_text or "No messages", inline=False)
        
        # Add ticket duration
        if ticket.get('created_at'):
            try:
                created = datetime.fromisoformat(ticket['created_at'].replace('Z', '+00:00'))
                duration = datetime.now(timezone.utc) - created
                hours, remainder = divmod(int(duration.total_seconds()), 3600)
                minutes, seconds = divmod(remainder, 60)
                duration_str = f"{hours}h {minutes}m {seconds}s" if hours > 0 else f"{minutes}m {seconds}s"
                embed.add_field(name="⏱️ Duration", value=duration_str, inline=True)
            except:
                pass
        
        embed.set_footer(text=f"Ticket ID: {ticket['ticket_id']} • Download HTML for full transcript")
        
        # Create file
        file = discord.File(
            BytesIO(html_content.encode('utf-8')),
            filename=f"transcript-{ticket['ticket_id']}.html"
        )
        
        return {'embed': embed, 'file': file, 'html': html_content}
    
    def _generate_html_transcript(self, channel: discord.TextChannel, ticket: Dict, messages: List[Dict], closed_by: discord.Member) -> str:
        """Generate a Ticket Tool style HTML transcript."""
        guild = channel.guild
        creator = guild.get_member(ticket['creator_id'])
        
        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Ticket Transcript - {ticket['ticket_id']}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            min-height: 100vh;
            color: #fff;
        }}
        .container {{ max-width: 900px; margin: 0 auto; padding: 20px; }}
        .header {{
            background: linear-gradient(135deg, #5865F2 0%, #7289DA 100%);
            padding: 30px;
            border-radius: 15px;
            margin-bottom: 20px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
        }}
        .header h1 {{ font-size: 28px; margin-bottom: 10px; }}
        .header .info {{ display: flex; gap: 20px; flex-wrap: wrap; margin-top: 15px; }}
        .header .info-item {{ 
            background: rgba(255,255,255,0.1);
            padding: 8px 15px;
            border-radius: 8px;
            font-size: 14px;
        }}
        .messages {{ background: #2f3136; border-radius: 15px; overflow: hidden; }}
        .message {{
            padding: 15px 20px;
            border-bottom: 1px solid #36393f;
            display: flex;
            gap: 15px;
        }}
        .message:hover {{ background: rgba(79, 84, 92, 0.16); }}
        .message:last-child {{ border-bottom: none; }}
        .message-avatar {{ width: 40px; height: 40px; border-radius: 50%; flex-shrink: 0; }}
        .message-content {{ flex: 1; }}
        .message-header {{ display: flex; align-items: center; gap: 10px; margin-bottom: 5px; }}
        .message-author {{ font-weight: 600; color: #fff; }}
        .message-timestamp {{ font-size: 12px; color: #72767d; }}
        .message-text {{ color: #dcddde; line-height: 1.5; word-wrap: break-word; }}
        .attachment {{
            background: #2f3136;
            border: 1px solid #4f545c;
            border-radius: 8px;
            padding: 10px;
            margin-top: 8px;
            display: inline-block;
        }}
        .attachment a {{ color: #00b0f4; text-decoration: none; }}
        .footer {{
            text-align: center;
            padding: 20px;
            color: #72767d;
            font-size: 14px;
        }}
        .claimed-badge {{
            background: #faa61a;
            color: #000;
            padding: 3px 8px;
            border-radius: 4px;
            font-size: 12px;
            margin-left: 10px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Ticket Transcript</h1>
            <div class="info">
                <div class="info-item">ID: {ticket['ticket_id']}</div>
                <div class="info-item">Category: {ticket.get('category', 'General')}</div>
                <div class="info-item">Created: {ticket.get('created_at', 'N/A')[:19]}</div>
                <div class="info-item">Closed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
            </div>
            <div class="info" style="margin-top: 10px;">
                <div class="info-item">Creator: {creator.display_name if creator else 'Unknown'}</div>
                <div class="info-item">Closed By: {closed_by.display_name}</div>
                {f'<div class="info-item">Claimed By: <@{ticket["claimed_by"]}></div>' if ticket.get('claimed_by') else ''}
            </div>
        </div>
        <div class="messages">
"""
        
        for msg in messages:
            html += f"""
            <div class="message">
                <img class="message-avatar" src="{msg['author_avatar']}" alt="Avatar">
                <div class="message-content">
                    <div class="message-header">
                        <span class="message-author">{msg['author_name']}</span>
                        <span class="message-timestamp">{msg['timestamp']}</span>
                    </div>
                    <div class="message-text">{msg['content'] or '<em>No content</em>'}</div>
"""
            if msg['attachments']:
                for att in msg['attachments']:
                    html += f"""
                    <div class="attachment"><a href="{att}" target="_blank">📎 Attachment</a></div>
"""
            html += """
                </div>
            </div>
"""
        
        html += f"""
        </div>
        <div class="footer">
            Generated by {self.bot.user.name} • {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </div>
    </div>
</body>
</html>"""
        
        return html


# Global ticket tool instance
ticket_tool: Optional[TicketToolSystem] = None


# --- TICKET PANEL VIEW (The panel message with create button) ---
class TicketPanelView(View):
    """The panel that users interact with to create tickets."""
    
    def __init__(self, panel: Dict):
        super().__init__(timeout=None)
        self.panel = panel
        
        # Configure button based on panel settings
        self.create_button.style = discord.ButtonStyle(panel.get('button_style', 3))
        self.create_button.label = panel.get('button_label', 'Create Ticket')
        if panel.get('button_emoji'):
            self.create_button.emoji = panel['button_emoji']
    
    @discord.ui.button(label="Create Ticket", style=discord.ButtonStyle.success, custom_id="create_ticket_button")
    async def create_button(self, interaction: discord.Interaction, button: Button) -> None:
        if not ticket_tool:
            await interaction.response.send_message("Ticket system not initialized.", ephemeral=True)
            return
        
        # Check blacklist
        blacklisted, reason = ticket_tool.data_manager.is_user_blacklisted(interaction.guild.id, interaction.user.id)
        if blacklisted:
            await interaction.response.send_message(f"You are blacklisted from creating tickets. Reason: {reason}", ephemeral=True)
            return
        
        # Check ticket limit from panel
        user_tickets = ticket_tool.data_manager.load_tickets_by_creator(interaction.user.id, interaction.guild.id)
        ticket_limit = self.panel.get('ticket_limit', 3)
        if len(user_tickets) >= ticket_limit:
            await interaction.response.send_message(
                f"You already have {len(user_tickets)} open ticket(s). Close one before creating another.",
                ephemeral=True
            )
            return
        
        # Check if panel has questions
        questions = ticket_tool.data_manager.load_panel_questions(self.panel['panel_id'])
        
        if questions:
            # Show modal with questions
            await interaction.response.send_modal(TicketQuestionsModal(self.panel, questions))
        else:
            # Create ticket directly
            await interaction.response.defer(thinking=True, ephemeral=True)
            channel, ticket_id = await ticket_tool.create_ticket(
                interaction.guild, interaction.user, self.panel
            )
            if channel:
                # Send welcome message
                await self._send_welcome_message(channel, interaction.user)
                await interaction.followup.send(f"Ticket created: {channel.mention}", ephemeral=True)
            else:
                await interaction.followup.send(f"Failed to create ticket: {ticket_id}", ephemeral=True)
    
    async def _send_welcome_message(self, channel: discord.TextChannel, user: discord.Member) -> None:
        """Send the welcome message in the ticket channel."""
        ticket = ticket_tool.data_manager.load_ticket_by_channel(channel.id)
        if not ticket:
            return
        
        # Create ticket action buttons
        view = TicketControlView(ticket['ticket_id'])
        
        # Get welcome message from panel or default
        welcome_text = self.panel.get('welcome_message', "Support will be with you shortly.")
        
        embed = discord.Embed(
            title=f"Ticket #{ticket['ticket_id']}",
            description=f"Welcome {user.mention}!\n\n{welcome_text}\n\n**Category:** {self.panel.get('name', 'General')}",
            color=discord.Color(self.panel.get('embed_color', 0x5865F2)),
            timestamp=datetime.now(timezone.utc)
        )
        
        if self.panel.get('embed_thumbnail'):
            embed.set_thumbnail(url=self.panel['embed_thumbnail'])
        if self.panel.get('embed_image'):
            embed.set_image(url=self.panel['embed_image'])
        
        embed.set_footer(text=f"Created by {user.display_name}")
        
        # Mention support role if configured
        mention_text = ""
        support_role_id = self.panel.get('support_role_id')
        if support_role_id:
            role = channel.guild.get_role(support_role_id)
            if role:
                mention_text = f"{role.mention} "
        
        await channel.send(f"{mention_text}{user.mention}", embed=embed, view=view)


class TicketQuestionsModal(Modal, title="Create Ticket"):
    """Modal for ticket questions."""
    
    def __init__(self, panel: Dict, questions: List[Dict]):
        super().__init__()
        self.panel = panel
        self.questions = questions
        self.answers = {}
        
        for i, q in enumerate(questions[:5]):  # Discord limits to 5 items
            text_input = TextInput(
                label=q['question_text'][:45],
                placeholder=q.get('placeholder', ''),
                style=discord.TextStyle.paragraph if q.get('question_type') == 'paragraph' else discord.TextStyle.short,
                required=q.get('required', True),
                custom_id=f"question_{q['question_id']}"
            )
            self.add_item(text_input)
    
    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(thinking=True, ephemeral=True)
        
        # Collect answers
        answers = {}
        for child in self.children:
            qid = child.custom_id.replace('question_', '')
            answers[qid] = child.value
        
        # Create ticket
        channel, ticket_id = await ticket_tool.create_ticket(
            interaction.guild, interaction.user, self.panel, answers=answers
        )
        
        if channel:
            # Send welcome message with answers
            await self._send_welcome_with_answers(channel, interaction.user, answers)
            await interaction.followup.send(f"Ticket created: {channel.mention}", ephemeral=True)
        else:
            await interaction.followup.send(f"Failed to create ticket: {ticket_id}", ephemeral=True)
    
    async def _send_welcome_with_answers(self, channel: discord.TextChannel, user: discord.Member, answers: Dict) -> None:
        """Send welcome message with question answers."""
        ticket = ticket_tool.data_manager.load_ticket_by_channel(channel.id)
        if not ticket:
            return
        
        view = TicketControlView(ticket['ticket_id'])
        
        welcome_text = self.panel.get('welcome_message', "Support will be with you shortly.")
        
        embed = discord.Embed(
            title=f"Ticket #{ticket['ticket_id']}",
            color=discord.Color(self.panel.get('embed_color', 0x5865F2)),
            timestamp=datetime.now(timezone.utc)
        )
        
        embed.add_field(name="Creator", value=user.mention, inline=True)
        embed.add_field(name="Category", value=self.panel.get('name', 'General'), inline=True)
        
        # Add answers - keys are bare question IDs (prefix stripped in on_submit)
        for q in self.questions:
            answer = answers.get(q['question_id'], 'No answer')
            embed.add_field(name=q['question_text'][:256], value=answer[:1024], inline=False)
        
        embed.add_field(name="Info", value=welcome_text, inline=False)
        embed.set_footer(text=f"Created by {user.display_name}")
        
        support_role_id = self.panel.get('support_role_id')
        mention_text = ""
        if support_role_id:
            role = channel.guild.get_role(support_role_id)
            if role:
                mention_text = f"{role.mention} "
        
        await channel.send(f"{mention_text}{user.mention}", embed=embed, view=view)


# --- TICKET CONTROL VIEW (Inside ticket channels) ---
class TicketControlView(View):
    """Buttons inside a ticket channel for control."""
    
    def __init__(self, ticket_id: str):
        super().__init__(timeout=None)
        self.ticket_id = ticket_id
    
    @discord.ui.button(label="Claim", style=discord.ButtonStyle.success, emoji="🙋")
    async def claim_button(self, interaction: discord.Interaction, button: Button) -> None:
        if not ticket_tool:
            await interaction.response.send_message("System not ready.", ephemeral=True)
            return
        
        success, message = await ticket_tool.claim_ticket(interaction.channel, interaction.user)
        
        if success:
            button.disabled = True
            button.label = "Claimed"
            button.style = discord.ButtonStyle.secondary
            await interaction.response.edit_message(view=self)
            await interaction.channel.send(f"Ticket claimed by {interaction.user.mention}")
        else:
            await interaction.response.send_message(message, ephemeral=True)
    
    @discord.ui.button(label="Close", style=discord.ButtonStyle.danger, emoji="🔒")
    async def close_button(self, interaction: discord.Interaction, button: Button) -> None:
        await interaction.response.send_modal(CloseTicketModal(self.ticket_id))
    
    @discord.ui.button(label="Transcript", style=discord.ButtonStyle.secondary, emoji="📜")
    async def transcript_button(self, interaction: discord.Interaction, button: Button) -> None:
        ticket = ticket_tool.data_manager.load_ticket(self.ticket_id)
        if not ticket:
            await interaction.response.send_message("Ticket not found.", ephemeral=True)
            return
        
        transcript = await ticket_tool._generate_transcript(
            interaction.channel, ticket, interaction.user
        )
        await interaction.response.send_message(
            embed=transcript['embed'],
            file=transcript['file'],
            ephemeral=True
        )

    @discord.ui.button(label="Note", style=discord.ButtonStyle.secondary, emoji="📝", row=1)
    async def note_button(self, interaction: discord.Interaction, button: Button) -> None:
        """Add a private staff note visible only to staff."""
        ticket = ticket_tool.data_manager.load_ticket(self.ticket_id)
        if not ticket:
            await interaction.response.send_message("Ticket not found.", ephemeral=True)
            return
        # Only staff / manage_channels can add notes
        if not interaction.user.guild_permissions.manage_channels:
            await interaction.response.send_message("Only staff can add notes.", ephemeral=True)
            return
        await interaction.response.send_modal(AddNoteModal(self.ticket_id))

    @discord.ui.button(label="Priority", style=discord.ButtonStyle.secondary, emoji="🚨", row=1)
    async def priority_button(self, interaction: discord.Interaction, button: Button) -> None:
        """Set the priority of this ticket."""
        if not interaction.user.guild_permissions.manage_channels:
            await interaction.response.send_message("Only staff can set priority.", ephemeral=True)
            return
        await interaction.response.send_message(
            "Select a priority level:",
            view=PrioritySelectView(self.ticket_id),
            ephemeral=True
        )


class CloseTicketModal(Modal, title="Close Ticket"):
    reason_input = TextInput(
        label="Close Reason",
        placeholder="Why are you closing this ticket?",
        style=discord.TextStyle.paragraph,
        required=False
    )
    
    def __init__(self, ticket_id: str):
        super().__init__()
        self.ticket_id = ticket_id
    
    async def on_submit(self, interaction: discord.Interaction) -> None:
        reason = self.reason_input.value or "No reason provided"
        
        # Check if this is the ticket creator
        ticket = ticket_tool.data_manager.load_ticket(self.ticket_id)
        is_creator = ticket and ticket.get('creator_id') == interaction.user.id
        
        if is_creator:
            # Show rating request BEFORE closing - only for ticket creator
            view = TicketRatingView(self.ticket_id, reason, interaction.channel, interaction.user)
            await interaction.response.send_message(
                "⭐ **Please rate your support experience before closing:**",
                view=view,
                ephemeral=True
            )
        else:
            # Staff closing - show confirmation
            await interaction.response.send_message(
                "Are you sure you want to close this ticket?",
                view=ConfirmCloseView(self.ticket_id, reason),
                ephemeral=True
            )


class ConfirmCloseView(View):
    """Confirmation view for staff closing tickets."""
    
    def __init__(self, ticket_id: str, reason: str):
        super().__init__(timeout=60)
        self.ticket_id = ticket_id
        self.reason = reason
    
    @discord.ui.button(label="✅ Close Ticket", style=discord.ButtonStyle.danger)
    async def confirm_close(self, interaction: discord.Interaction, button: Button) -> None:
        await interaction.response.defer(thinking=True)
        
        # Disable buttons
        for child in self.children:
            child.disabled = True
        await interaction.edit_original_response(view=self)
        
        # Now close the ticket
        success = await ticket_tool.close_ticket(interaction.channel, interaction.user, self.reason)
        if not success:
            await interaction.followup.send("Failed to close ticket.", ephemeral=True)
    
    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_close(self, interaction: discord.Interaction, button: Button) -> None:
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(
            content="Close cancelled.",
            view=self
        )


class TicketRatingView(View):
    """Rating view BEFORE ticket close - gives user time to rate."""
    
    def __init__(self, ticket_id: str, reason: str, channel: discord.TextChannel, user: discord.Member):
        super().__init__(timeout=120)  # 2 minutes to rate
        self.ticket_id = ticket_id
        self.reason = reason
        self.channel = channel
        self.user = user
        self.rated = False
    
    async def on_timeout(self) -> None:
        """Close ticket after timeout if not rated."""
        if not self.rated and self.channel:
            try:
                await ticket_tool.close_ticket(self.channel, self.user, self.reason)
            except:
                pass  # Channel might already be deleted
    
    @discord.ui.button(label="⭐", style=discord.ButtonStyle.secondary, row=0)
    async def rate_1(self, interaction: discord.Interaction, button: Button) -> None:
        await self._submit_rating(interaction, 1)
    
    @discord.ui.button(label="⭐⭐", style=discord.ButtonStyle.secondary, row=0)
    async def rate_2(self, interaction: discord.Interaction, button: Button) -> None:
        await self._submit_rating(interaction, 2)
    
    @discord.ui.button(label="⭐⭐⭐", style=discord.ButtonStyle.secondary, row=0)
    async def rate_3(self, interaction: discord.Interaction, button: Button) -> None:
        await self._submit_rating(interaction, 3)
    
    @discord.ui.button(label="⭐⭐⭐⭐", style=discord.ButtonStyle.secondary, row=1)
    async def rate_4(self, interaction: discord.Interaction, button: Button) -> None:
        await self._submit_rating(interaction, 4)
    
    @discord.ui.button(label="⭐⭐⭐⭐⭐", style=discord.ButtonStyle.success, row=1)
    async def rate_5(self, interaction: discord.Interaction, button: Button) -> None:
        await self._submit_rating(interaction, 5)
    
    @discord.ui.button(label="Skip & Close", style=discord.ButtonStyle.danger, row=2)
    async def skip_rating(self, interaction: discord.Interaction, button: Button) -> None:
        self.rated = True
        await interaction.response.defer(thinking=True)
        
        # Disable all buttons
        for child in self.children:
            child.disabled = True
        await interaction.edit_original_response(
            content="Closing ticket without rating...",
            view=self
        )
        
        # Close the ticket
        success = await ticket_tool.close_ticket(self.channel, self.user, self.reason)
        if not success:
            await interaction.followup.send("Failed to close ticket.", ephemeral=True)
    
    async def _submit_rating(self, interaction: discord.Interaction, rating: int) -> None:
        self.rated = True
        
        # Save rating
        ticket = ticket_tool.data_manager.load_ticket(self.ticket_id)
        if ticket:
            ticket['rating'] = rating
            ticket_tool.data_manager.save_ticket(ticket)
        
        # Disable all buttons
        for child in self.children:
            child.disabled = True
        
        await interaction.response.edit_message(
            content=f"⭐ Thank you for rating! You gave **{rating} star(s)**.\n\nClosing ticket in 3 seconds...",
            view=self
        )
        
        # Wait a moment then close
        await asyncio.sleep(3)
        
        # Close the ticket
        try:
            await ticket_tool.close_ticket(self.channel, self.user, self.reason)
        except:
            pass  # Channel might already be deleted


# --- ADD NOTE MODAL ---
class AddNoteModal(Modal, title="Add Staff Note"):
    note_input = TextInput(
        label="Note (only staff can see this)",
        style=discord.TextStyle.paragraph,
        placeholder="Internal note visible only to staff...",
        max_length=1000,
        required=True
    )

    def __init__(self, ticket_id: str):
        super().__init__()
        self.ticket_id = ticket_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        import uuid
        note = {
            'note_id': str(uuid.uuid4())[:8],
            'ticket_id': self.ticket_id,
            'guild_id': interaction.guild.id,
            'author_id': interaction.user.id,
            'content': self.note_input.value,
            'created_at': datetime.now(timezone.utc).isoformat(),
        }
        data_manager.save_ticket_note(note)

        embed = discord.Embed(
            title="📝 Staff Note Added",
            description=self.note_input.value,
            color=discord.Color.yellow(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_footer(text=f"Added by {interaction.user.display_name} • Note ID: {note['note_id']}")
        # Send ephemeral so only staff see it (staff already in the channel see channel, but
        # this confirms the note was saved without broadcasting to the ticket creator)
        await interaction.response.send_message(embed=embed, ephemeral=True)
        # Also post visibly but with a staff-only ping so the channel log shows it
        settings = data_manager.load_ticket_settings(interaction.guild.id)
        support_role_id = settings.get('support_role_id') if settings else None
        if support_role_id:
            role = interaction.guild.get_role(support_role_id)
            if role:
                note_embed = discord.Embed(
                    title="📝 Staff Note",
                    description=self.note_input.value,
                    color=discord.Color.yellow(),
                    timestamp=datetime.now(timezone.utc)
                )
                note_embed.set_footer(text=f"By {interaction.user.display_name}")
                # Override channel perms so only staff see this message isn't possible via API,
                # but we post it so it appears in the transcript for staff. Ticket creator
                # can technically see it — use /notes for private-only review.
                await interaction.channel.send(embed=note_embed)


# --- PRIORITY SELECT VIEW ---
PRIORITY_COLORS = {
    'low':    discord.Color.green(),
    'normal': discord.Color.blue(),
    'high':   discord.Color.orange(),
    'urgent': discord.Color.red(),
}
PRIORITY_EMOJIS = {'low': '🟢', 'normal': '🔵', 'high': '🟠', 'urgent': '🔴'}


class PrioritySelectView(View):
    def __init__(self, ticket_id: str):
        super().__init__(timeout=60)
        self.ticket_id = ticket_id

    @discord.ui.button(label="🟢 Low", style=discord.ButtonStyle.success)
    async def low(self, interaction: discord.Interaction, button: Button) -> None:
        await self._set_priority(interaction, 'low')

    @discord.ui.button(label="🔵 Normal", style=discord.ButtonStyle.primary)
    async def normal(self, interaction: discord.Interaction, button: Button) -> None:
        await self._set_priority(interaction, 'normal')

    @discord.ui.button(label="🟠 High", style=discord.ButtonStyle.secondary)
    async def high(self, interaction: discord.Interaction, button: Button) -> None:
        await self._set_priority(interaction, 'high')

    @discord.ui.button(label="🔴 Urgent", style=discord.ButtonStyle.danger)
    async def urgent(self, interaction: discord.Interaction, button: Button) -> None:
        await self._set_priority(interaction, 'urgent')

    async def _set_priority(self, interaction: discord.Interaction, priority: str) -> None:
        ticket = data_manager.load_ticket(self.ticket_id)
        if not ticket:
            await interaction.response.send_message("Ticket not found.", ephemeral=True)
            return
        ticket['priority'] = priority
        data_manager.save_ticket(ticket)

        emoji = PRIORITY_EMOJIS.get(priority, '')
        color = PRIORITY_COLORS.get(priority, discord.Color.blue())
        embed = discord.Embed(
            title=f"{emoji} Priority Set: {priority.capitalize()}",
            color=color,
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_footer(text=f"Set by {interaction.user.display_name}")
        await interaction.response.edit_message(content=None, embed=embed, view=None)
        await interaction.channel.send(
            embed=discord.Embed(
                description=f"{emoji} Ticket priority set to **{priority.capitalize()}** by {interaction.user.mention}",
                color=color
            )
        )
        self.stop()


# --- PANEL CREATOR VIEW (For creating/editing panels) ---
class PanelCreatorView(View):
    """Interactive panel creator."""
    
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.user_id = user_id
        self.panel_data = {
            'panel_id': str(uuid.uuid4())[:8],
            'guild_id': guild_id,
            'name': 'New Panel',
            'embed_title': 'Support Tickets',
            'embed_description': 'Click the button below to create a ticket.',
            'embed_color': 0x5865F2,
            'button_label': 'Create Ticket',
            'button_style': 3,
            'ticket_limit': 3,
            'auto_close_hours': 24,
            'welcome_message': 'Support will be with you shortly.',
            'created_at': datetime.now(timezone.utc).isoformat()
        }
    
    def _create_preview_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title=self.panel_data['embed_title'],
            description=self.panel_data['embed_description'],
            color=discord.Color(self.panel_data['embed_color'])
        )
        embed.set_footer(text=f"Panel: {self.panel_data['name']}")
        return embed
    
    @discord.ui.button(label="Set Name", style=discord.ButtonStyle.primary, row=0)
    async def set_name(self, interaction: discord.Interaction, button: Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Not your panel creator.", ephemeral=True)
            return
        await interaction.response.send_modal(PanelNameModal(self))
    
    @discord.ui.button(label="Set Embed", style=discord.ButtonStyle.primary, row=0)
    async def set_embed(self, interaction: discord.Interaction, button: Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Not your panel creator.", ephemeral=True)
            return
        await interaction.response.send_modal(PanelEmbedModal(self))
    
    @discord.ui.button(label="Set Button", style=discord.ButtonStyle.secondary, row=0)
    async def set_button(self, interaction: discord.Interaction, button: Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Not your panel creator.", ephemeral=True)
            return
        await interaction.response.send_modal(PanelButtonModal(self))
    
    @discord.ui.button(label="Settings", style=discord.ButtonStyle.secondary, row=1)
    async def settings(self, interaction: discord.Interaction, button: Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Not your panel creator.", ephemeral=True)
            return
        await interaction.response.send_modal(PanelSettingsModal(self))
    
    @discord.ui.button(label="Preview", style=discord.ButtonStyle.success, row=1)
    async def preview(self, interaction: discord.Interaction, button: Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Not your panel creator.", ephemeral=True)
            return
        
        preview_view = View(timeout=30)
        preview_view.add_item(Button(
            label=self.panel_data['button_label'],
            style=discord.ButtonStyle(self.panel_data['button_style']),
            disabled=True
        ))
        
        await interaction.response.send_message(
            embed=self._create_preview_embed(),
            view=preview_view,
            ephemeral=True
        )
    
    @discord.ui.button(label="Create Panel", style=discord.ButtonStyle.success, row=2)
    async def create_panel(self, interaction: discord.Interaction, button: Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Not your panel creator.", ephemeral=True)
            return
        
        # Save panel
        self.panel_data['channel_id'] = interaction.channel.id
        data_manager.save_ticket_panel(self.panel_data)
        
        # Send the actual panel
        view = TicketPanelView(self.panel_data)
        message = await interaction.channel.send(
            embed=self._create_preview_embed(),
            view=view
        )
        
        # Update panel with message ID
        self.panel_data['message_id'] = message.id
        data_manager.save_ticket_panel(self.panel_data)
        
        # Register view for persistence
        interaction.client.add_view(view)
        
        await interaction.response.send_message(
            f"Panel created successfully! Panel ID: `{self.panel_data['panel_id']}`",
            ephemeral=True
        )
        self.stop()


class PanelNameModal(Modal, title="Panel Name"):
    name_input = TextInput(label="Panel Name", placeholder="e.g., Support Tickets", max_length=50)
    
    def __init__(self, view: PanelCreatorView):
        super().__init__()
        self.view = view
        self.name_input.default = view.panel_data.get('name', '')
    
    async def on_submit(self, interaction: discord.Interaction) -> None:
        self.view.panel_data['name'] = self.name_input.value
        await interaction.response.send_message(f"Panel name set to: {self.name_input.value}", ephemeral=True)


class PanelEmbedModal(Modal, title="Embed Settings"):
    title_input = TextInput(label="Embed Title", max_length=100)
    desc_input = TextInput(label="Embed Description", style=discord.TextStyle.paragraph, max_length=1000, required=False)
    color_input = TextInput(label="Color (Hex)", max_length=7, placeholder="#5865F2", required=False)
    
    def __init__(self, view: PanelCreatorView):
        super().__init__()
        self.view = view
        self.title_input.default = view.panel_data.get('embed_title', '')
        self.desc_input.default = view.panel_data.get('embed_description', '')
        self.color_input.default = hex(view.panel_data.get('embed_color', 0x5865F2))[2:]
    
    async def on_submit(self, interaction: discord.Interaction) -> None:
        self.view.panel_data['embed_title'] = self.title_input.value
        
        if self.desc_input.value:
            self.view.panel_data['embed_description'] = self.desc_input.value
        
        if self.color_input.value:
            try:
                color_hex = self.color_input.value.strip('#')
                self.view.panel_data['embed_color'] = int(color_hex, 16)
            except ValueError:
                pass
        
        await interaction.response.send_message("Embed settings updated!", ephemeral=True)


class PanelButtonModal(Modal, title="Button Settings"):
    label_input = TextInput(label="Button Label", max_length=80, placeholder="Create Ticket")
    emoji_input = TextInput(label="Button Emoji", max_length=50, required=False, placeholder="🎫")
    style_input = TextInput(label="Style (1-4)", max_length=1, placeholder="3")
    
    def __init__(self, view: PanelCreatorView):
        super().__init__()
        self.view = view
        self.label_input.default = view.panel_data.get('button_label', '')
        self.emoji_input.default = view.panel_data.get('button_emoji', '')
        self.style_input.default = str(view.panel_data.get('button_style', 3))
    
    async def on_submit(self, interaction: discord.Interaction) -> None:
        self.view.panel_data['button_label'] = self.label_input.value
        
        if self.emoji_input.value:
            self.view.panel_data['button_emoji'] = self.emoji_input.value
        
        try:
            style = int(self.style_input.value)
            if 1 <= style <= 4:
                self.view.panel_data['button_style'] = style
        except ValueError:
            pass
        
        await interaction.response.send_message("Button settings updated!", ephemeral=True)


class PanelSettingsModal(Modal, title="Panel Settings"):
    limit_input = TextInput(label="Max Tickets Per User", max_length=2, placeholder="3")
    close_input = TextInput(label="Auto-Close Hours", max_length=3, placeholder="24")
    welcome_input = TextInput(label="Welcome Message", style=discord.TextStyle.paragraph, max_length=500, required=False)
    
    def __init__(self, view: PanelCreatorView):
        super().__init__()
        self.view = view
        self.limit_input.default = str(view.panel_data.get('ticket_limit', 3))
        self.close_input.default = str(view.panel_data.get('auto_close_hours', 24))
        self.welcome_input.default = view.panel_data.get('welcome_message', '')
    
    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            self.view.panel_data['ticket_limit'] = int(self.limit_input.value)
        except ValueError:
            pass
        
        try:
            self.view.panel_data['auto_close_hours'] = int(self.close_input.value)
        except ValueError:
            pass
        
        if self.welcome_input.value:
            self.view.panel_data['welcome_message'] = self.welcome_input.value
        
        await interaction.response.send_message("Settings updated!", ephemeral=True)


import uuid





# --- GIVEAWAY SYSTEM (V2 Enhancement) ---
class GiveawayView(View):
    def __init__(self, giveaway_id: str):
        super().__init__(timeout=None)
        self.giveaway_id = giveaway_id
    
    @discord.ui.button(label="ðŸŽ‰ Enter Giveaway", style=discord.ButtonStyle.success)
    async def enter_button(self, interaction: discord.Interaction, button: Button) -> None:
        if self.giveaway_id not in giveaways_data:
            await interaction.response.send_message("This giveaway no longer exists.", ephemeral=True)
            return
        
        giveaway = giveaways_data[self.giveaway_id]
        
        if interaction.user.id in giveaway.get('entries', []):
            await interaction.response.send_message("You've already entered this giveaway!", ephemeral=True)
            return
        
        if 'entries' not in giveaway:
            giveaway['entries'] = []
        
        giveaway['entries'].append(interaction.user.id)
        save_giveaways_data()
        
        button.label = f"ðŸŽ‰ Entered ({len(giveaway['entries'])})"
        try:
            await interaction.message.edit(view=self)
        except:
            pass
        
        await interaction.response.send_message("You've been entered! Good luck! ðŸ€", ephemeral=True)


# --- AUTOCOMPLETE FUNCTIONS ---
async def blacklist_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    matching_keywords = [keyword for keyword in blacklisted_keywords if current.lower() in keyword.lower()][:25]
    return [app_commands.Choice(name=keyword[:100], value=keyword[:100]) for keyword in matching_keywords]


async def invite_code_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    if not invite_manager or not invite_manager.tracked_invites:
        return []
    matching_invites = [code for code in invite_manager.tracked_invites.keys() if current.lower() in code.lower()][:25]
    return [app_commands.Choice(name=f"{code} ({invite_manager.tracked_invites[code].get('name', 'Unknown')})", value=code) for code in matching_invites]


# --- INVITE MANAGER CLASS ---
class InviteManager:
    def __init__(self, bot_instance: commands.Bot):
        self.bot = bot_instance
        self.invite_message_id: Optional[int] = None
        self.invite_channel_id: Optional[int] = None
        self.tracked_invites: Dict[str, Dict[str, Any]] = {}
        self.guild_invites: Dict[str, discord.Invite] = {}
    
    def load_data(self) -> None:
        try:
            self.invite_message_id, self.invite_channel_id, self.tracked_invites = data_manager.load_invites()
            logging.info(f"[InviteManager] Loaded {len(self.tracked_invites)} tracked invites from SQLite")
        except Exception as e:
            logging.error(f"[InviteManager] Error loading data: {e}")

    def save_data(self) -> None:
        try:
            data_manager.save_invites(self.invite_message_id, self.invite_channel_id, self.tracked_invites)
        except Exception as e:
            logging.error(f"[InviteManager] Error saving data: {e}")
    
    async def create_invite(self, guild: discord.Guild, max_uses: int, max_age: int = 0) -> Optional[discord.Invite]:
        for channel in guild.text_channels:
            if channel.permissions_for(guild.me).create_instant_invite:
                invite = await channel.create_invite(max_uses=max_uses, max_age=max_age, unique=True, reason="Auto-generated by MOS Invite Manager")
                logging.info(f"[InviteManager] Created invite {invite.code} with {max_uses} max uses")
                return invite
        return None
    
    async def check_invite_status(self, guild: discord.Guild, invite_code: str) -> Dict[str, Any]:
        try:
            invites = await guild.invites()
            for invite in invites:
                if invite.code == invite_code:
                    return {'valid': True, 'uses': invite.uses, 'max_uses': invite.max_uses, 'expired': invite.uses >= invite.max_uses if invite.max_uses else False}
            return {'valid': False, 'uses': self.tracked_invites.get(invite_code, {}).get('max_uses', 0), 'max_uses': self.tracked_invites.get(invite_code, {}).get('max_uses', 0), 'expired': True}
        except Exception as e:
            logging.error(f"[InviteManager] Error checking invite {invite_code}: {e}")
            return {'valid': False, 'expired': True, 'uses': 0, 'max_uses': 0}
    
    async def generate_all_invites(self, guild: discord.Guild) -> Dict[str, Dict[str, Any]]:
        new_invites: Dict[str, Dict[str, Any]] = {}
        for cfg in INVITE_CONFIGS:
            invite = await self.create_invite(guild, cfg['max_uses'], cfg['max_age'])
            if invite:
                new_invites[invite.code] = {'name': cfg['name'], 'max_uses': cfg['max_uses'], 'uses': 0, 'status': 'active', 'created_at': datetime.now(timezone.utc).isoformat()}
        return new_invites
    
    async def update_invite_message(self, channel: discord.TextChannel) -> bool:
        if not self.invite_message_id:
            return False
        try:
            message = await channel.fetch_message(self.invite_message_id)
        except discord.NotFound:
            self.invite_message_id = None
            return False
        
        try:
            live_invites = await channel.guild.invites()
            live_invite_data = {inv.code: inv for inv in live_invites}
        except Exception:
            live_invite_data = {}
        
        all_expired = True
        active_invites: List[str] = []
        expired_invites: List[str] = []
        
        for code, data in self.tracked_invites.items():
            max_uses = data.get('max_uses', 0)
            if data['status'] == 'expired' or data.get('expired', False):
                expired_invites.append(f"discord.gg/{code} - EXPIRED")
            else:
                current_uses = live_invite_data[code].uses if code in live_invite_data else data.get('uses', 0)
                remaining = max_uses - current_uses
                if remaining <= 0 and max_uses > 0:
                    expired_invites.append(f"discord.gg/{code} - EXPIRED (0/{max_uses} Remaining)")
                    self.tracked_invites[code]['status'] = 'expired'
                    self.tracked_invites[code]['expired'] = True
                else:
                    if max_uses > 0:
                        active_invites.append(f"discord.gg/{code} - {remaining}/{max_uses} Uses Remaining")
                    else:
                        active_invites.append(f"discord.gg/{code} - Unlimited Uses ({current_uses} used)")
                    all_expired = False
        
        embed = self._build_invite_embed(active_invites, expired_invites, all_expired)
        embed.set_footer(text=f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        await message.edit(embed=embed)
        self.save_data()
        return True
    
    def _build_invite_embed(self, active_invites: List[str], expired_invites: List[str], all_expired: bool) -> discord.Embed:
        description_parts = ["***Invite Links:***\n"]
        for inv in active_invites:
            description_parts.append(f"```\n{inv}\n```")
        for inv in expired_invites:
            description_parts.append(f"```diff\n- {inv}\n```")
        
        embed = discord.Embed(description="".join(description_parts), color=discord.Color.dark_theme())
        
        if all_expired and self.tracked_invites:
            embed.add_field(name="Status", value="All invites expired! Use `!regenerateinvites` to create new ones.", inline=False)
            embed.color = discord.Color.red()
        elif active_invites:
            embed.add_field(name="Info", value=f"Tracking **{len(active_invites)}** active invite(s)", inline=False)
        
        return embed
    
    async def post_initial_message(self, channel: discord.TextChannel) -> discord.Message:
        embed = discord.Embed(description="***Invite Links:***\nSetting up invite links...", color=discord.Color.gold())
        message = await channel.send(embed=embed)
        self.invite_message_id = message.id
        self.invite_channel_id = channel.id
        self.save_data()
        return message
    
    async def check_and_update_all(self, guild: discord.Guild, channel: discord.TextChannel) -> bool:
        any_changed = False
        for code in list(self.tracked_invites.keys()):
            status = await self.check_invite_status(guild, code)
            if status['expired'] and self.tracked_invites[code]['status'] != 'expired':
                self.tracked_invites[code]['status'] = 'expired'
                self.tracked_invites[code]['expired'] = True
                self.tracked_invites[code]['uses'] = status['max_uses']
                any_changed = True
                logging.info(f"[InviteManager] Invite {code} has expired")
            elif status['valid']:
                self.tracked_invites[code]['uses'] = status['uses']
                if self.tracked_invites[code]['status'] != 'active':
                    self.tracked_invites[code]['status'] = 'active'
                    any_changed = True
        if any_changed:
            self.save_data()
            await self.update_invite_message(channel)
        return any_changed
    
    async def regenerate_expired(self, guild: discord.Guild, channel: discord.TextChannel) -> bool:
        if not self.tracked_invites:
            return False
        all_expired = all(data.get('expired', False) or data['status'] == 'expired' for data in self.tracked_invites.values())
        if all_expired:
            logging.info("[InviteManager] All invites expired, regenerating...")
            new_invites = await self.generate_all_invites(guild)
            if not new_invites:
                return False
            for code in self.tracked_invites:
                self.tracked_invites[code]['expired'] = True
                self.tracked_invites[code]['status'] = 'expired'
            for code, data in new_invites.items():
                self.tracked_invites[code] = data
            self.save_data()
            await self.update_invite_message(channel)
            return True
        return False


# --- DATA PERSISTENCE FUNCTIONS ---
def load_blacklist_data() -> None:
    global blacklisted_keywords
    try:
        blacklisted_keywords = data_manager.load_blacklist()
        logging.info(f"[Blacklist] Loaded {len(blacklisted_keywords)} blacklisted keywords from SQLite")
    except Exception as e:
        logging.error(f"[Blacklist] Error loading data: {e}")
        blacklisted_keywords = set()


def save_blacklist_data() -> None:
    try:
        data_manager.save_blacklist(blacklisted_keywords)
    except Exception as e:
        logging.error(f"[Blacklist] Error saving data: {e}")


def load_rules_cache() -> None:
    global rules_cache
    try:
        rules_cache = data_manager.load_rules_cache()
        logging.info("[RulesCache] Loaded cached rules from SQLite")
    except Exception as e:
        logging.error(f"[RulesCache] Error loading: {e}")


def save_rules_cache() -> None:
    try:
        data_manager.save_rules_cache(rules_cache)
    except Exception as e:
        logging.error(f"[RulesCache] Error saving: {e}")


def save_tickets_data() -> None:
    try:
        with open(config.tickets_data_file, 'w') as f:
            json.dump(tickets_data, f, indent=2)
    except Exception as e:
        logging.error(f"[Tickets] Error saving data: {e}")


def load_tickets_data() -> None:
    """Load tickets data - resets on bot restart (temporary data only)."""
    global tickets_data
    # TICKETS RESET ON BOT START - Start fresh
    tickets_data = {}
    logging.info("[Tickets] Tickets data reset (fresh start)")


def reset_tickets_data() -> None:
    """Clear tickets data and delete the file."""
    global tickets_data
    tickets_data = {}
    try:
        if os.path.exists(config.tickets_data_file):
            os.remove(config.tickets_data_file)
            logging.info("[Tickets] Tickets data file deleted")
    except Exception as e:
        logging.error(f"[Tickets] Error resetting data: {e}")


def save_giveaways_data() -> None:
    try:
        with open(config.giveaways_data_file, 'w') as f:
            json.dump(giveaways_data, f, indent=2)
    except Exception as e:
        logging.error(f"[Giveaways] Error saving data: {e}")


def load_giveaways_data() -> None:
    """Load giveaways data - resets on bot restart (temporary data only)."""
    global giveaways_data
    # GIVEAWAYS RESET ON BOT START - Start fresh
    giveaways_data = {}
    logging.info("[Giveaways] Giveaways data reset (fresh start)")


def reset_giveaways_data() -> None:
    """Clear giveaways data and delete the file."""
    global giveaways_data
    giveaways_data = {}
    try:
        if os.path.exists(config.giveaways_data_file):
            os.remove(config.giveaways_data_file)
            logging.info("[Giveaways] Giveaways data file deleted")
    except Exception as e:
        logging.error(f"[Giveaways] Error resetting data: {e}")


def save_levels_data() -> None:
    try:
        data_manager.save_all_levels(levels_data)
    except Exception as e:
        logging.error(f"[Levels] Error saving data: {e}")


def load_levels_data() -> None:
    global levels_data
    try:
        levels_data = data_manager.load_all_levels()
        logging.info(f"[Levels] Loaded {len(levels_data)} user levels from SQLite")
    except Exception as e:
        logging.error(f"[Levels] Error loading data: {e}")


def save_all_data() -> None:
    """Save all persistent data. Tickets and giveaways are NOT saved (temporary)."""
    if invite_manager:
        invite_manager.save_data()
    save_blacklist_data()
    save_rules_cache()
    save_levels_data()
    logging.info("[DataManager] All persistent data saved to SQLite.")


def reset_temporary_data() -> None:
    """Reset temporary data (tickets and giveaways) on bot shutdown."""
    reset_tickets_data()
    reset_giveaways_data()
    logging.info("[DataManager] Temporary data (tickets, giveaways) reset.")


def import_json_to_sqlite() -> None:
    """
    Import existing JSON files into SQLite database.
    This runs once on startup if JSON files are found.
    After import, JSON files are renamed to .bak to prevent re-import.
    """
    imported_something = False
    
    # === IMPORT INVITES ===
    if os.path.exists('invite_data.json'):
        try:
            with open('invite_data.json', 'r') as f:
                data = json.load(f)
            
            message_id = data.get('message_id')
            channel_id = data.get('channel_id')
            tracked_invites = data.get('tracked_invites', {})
            
            data_manager.save_invites(message_id, channel_id, tracked_invites)
            
            # Rename to .bak
            os.rename('invite_data.json', 'invite_data.json.bak')
            
            logging.info(f"[Import] Imported {len(tracked_invites)} invite(s) from invite_data.json")
            imported_something = True
        except Exception as e:
            logging.error(f"[Import] Error importing invites: {e}")
    
    # === IMPORT LEVELS ===
    if os.path.exists('levels_data.json'):
        try:
            with open('levels_data.json', 'r') as f:
                data = json.load(f)
            
            count = 0
            for key, value in data.items():
                parts = key.split('_')
                if len(parts) == 2:
                    user_id = int(parts[0])
                    guild_id = int(parts[1])
                    data_manager.save_level(
                        user_id, guild_id,
                        value.get('xp', 0),
                        value.get('level', 0),
                        value.get('total_messages', 0),
                        value.get('last_xp_gain')
                    )
                    count += 1
            
            # Rename to .bak
            os.rename('levels_data.json', 'levels_data.json.bak')
            
            logging.info(f"[Import] Imported {count} user level(s) from levels_data.json")
            imported_something = True
        except Exception as e:
            logging.error(f"[Import] Error importing levels: {e}")
    
    # === IMPORT RULES CACHE ===
    if os.path.exists('rules_cache.json'):
        try:
            with open('rules_cache.json', 'r') as f:
                data = json.load(f)
            
            data_manager.save_rules_cache(data)
            
            # Rename to .bak
            os.rename('rules_cache.json', 'rules_cache.json.bak')
            
            logging.info(f"[Import] Imported rules cache from rules_cache.json")
            imported_something = True
        except Exception as e:
            logging.error(f"[Import] Error importing rules cache: {e}")
    
    # === IMPORT BLACKLIST ===
    if os.path.exists('blacklist_data.json'):
        try:
            with open('blacklist_data.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            keywords = set(data.get('blacklisted_keywords', []))
            data_manager.save_blacklist(keywords)
            
            # Rename to .bak
            os.rename('blacklist_data.json', 'blacklist_data.json.bak')
            
            logging.info(f"[Import] Imported {len(keywords)} blacklist keyword(s) from blacklist_data.json")
            imported_something = True
        except Exception as e:
            logging.error(f"[Import] Error importing blacklist: {e}")
    
    # Move giveaways to proper location if found in root
    if os.path.exists('giveaways_data.json'):
        try:
            os.makedirs(config.json_dir, exist_ok=True)
            dest = os.path.join(config.json_dir, 'giveaways_data.json')
            if not os.path.exists(dest):
                os.rename('giveaways_data.json', dest)
                logging.info(f"[Import] Moved giveaways_data.json to {config.json_dir}")
        except Exception as e:
            logging.error(f"[Import] Error moving giveaways: {e}")
    
    # Move tickets to proper location if found in root
    if os.path.exists('tickets_data.json'):
        try:
            os.makedirs(config.json_dir, exist_ok=True)
            dest = os.path.join(config.json_dir, 'tickets_data.json')
            if not os.path.exists(dest):
                os.rename('tickets_data.json', dest)
                logging.info(f"[Import] Moved tickets_data.json to {config.json_dir}")
        except Exception as e:
            logging.error(f"[Import] Error moving tickets: {e}")
    
    if imported_something:
        logging.info("[Import] JSON import complete! Old files renamed to .bak")
    else:
        logging.info("[Import] No JSON files found to import")


# --- BLACKLIST HELPERS ---
def check_text_for_keywords(text: str) -> Tuple[bool, Optional[str]]:
    if not text:
        return (False, None)
    text_lower = text.lower()
    for keyword in blacklisted_keywords:
        if keyword.lower() in text_lower:
            return (True, keyword)
    return (False, None)


async def check_user_profile_for_blacklist(member: discord.Member) -> Tuple[bool, Optional[str], Optional[str]]:
    if not blacklisted_keywords:
        return (False, None, None)
    
    if isinstance(member, discord.Member) and member.activities:
        for activity in member.activities:
            if activity.type == discord.ActivityType.custom:
                parts = []
                if hasattr(activity, 'state') and activity.state:
                    parts.append(activity.state)
                if hasattr(activity, 'name') and activity.name:
                    parts.append(activity.name)
                if hasattr(activity, 'emoji') and activity.emoji and activity.emoji.name:
                    parts.append(activity.emoji.name)
                status_text = ' '.join(parts).strip()
                if status_text:
                    found, keyword = check_text_for_keywords(status_text)
                    if found:
                        return (True, keyword, "Custom Status")
    
    found, keyword = check_text_for_keywords(member.display_name)
    if found:
        return (True, keyword, "Display Name")
    
    found, keyword = check_text_for_keywords(member.name)
    if found:
        return (True, keyword, "Username")
    
    return (False, None, None)


async def auto_ban_if_blacklisted(member: discord.Member, source: str = "unknown") -> bool:
    is_blacklisted, keyword, location = await check_user_profile_for_blacklist(member)
    if not is_blacklisted:
        return False
    try:
        await member.ban(reason=f"Auto-banned: Blacklisted keyword '{keyword}' found in {location}")
        logging.info(f"[Blacklist] Auto-banned {member} (ID: {member.id}) via {source} - Keyword '{keyword}' in {location}")
        
        log_channel = bot.get_channel(config.channels.log)
        if log_channel:
            embed = discord.Embed(title="Auto-Ban: Blacklisted Keyword Detected", color=discord.Color.red())
            embed.add_field(name="User", value=f"{member.mention} ({member.name})", inline=True)
            embed.add_field(name="User ID", value=str(member.id), inline=True)
            embed.add_field(name="Matched Keyword", value=f"**{keyword}**", inline=True)
            embed.add_field(name="Location", value=location, inline=True)
            embed.add_field(name="Triggered By", value=source, inline=True)
            embed.timestamp = datetime.now(timezone.utc)
            await log_channel.send(embed=embed)
        return True
    except discord.Forbidden:
        logging.warning(f"[Blacklist] Failed to ban {member} - No permission")
    except discord.HTTPException as e:
        logging.error(f"[Blacklist] Failed to ban {member} - HTTP Error: {e}")
    return False


async def scan_and_ban_blacklisted_members(guild: discord.Guild) -> Tuple[int, int, List[Dict[str, Any]]]:
    banned_count = 0
    failed_count = 0
    matches: List[Dict[str, Any]] = []
    
    for member in guild.members:
        if member.bot:
            continue
        is_blacklisted, keyword, location = await check_user_profile_for_blacklist(member)
        if is_blacklisted:
            matches.append({'user': member, 'keyword': keyword, 'location': location})
            try:
                await member.ban(reason=f"Auto-banned: Blacklisted keyword '{keyword}' found in {location}")
                banned_count += 1
                logging.info(f"[Blacklist] Banned {member} (ID: {member.id}) - Keyword '{keyword}' in {location}")
            except discord.Forbidden:
                failed_count += 1
                logging.warning(f"[Blacklist] Failed to ban {member} - No permission")
            except discord.HTTPException as e:
                failed_count += 1
                logging.error(f"[Blacklist] Failed to ban {member} - HTTP Error: {e}")
    
    return (banned_count, failed_count, matches)


# --- BACKGROUND TASKS ---
@tasks.loop(minutes=config.timing.invite_check_interval_minutes)
async def check_invites_task() -> None:
    global invite_manager
    if not invite_manager or not invite_manager.invite_channel_id:
        return
    channel = invite_manager.bot.get_channel(invite_manager.invite_channel_id)
    if not channel:
        return
    guild = channel.guild
    await invite_manager.check_and_update_all(guild, channel)
    await invite_manager.regenerate_expired(guild, channel)


@check_invites_task.before_loop
async def before_check_invites() -> None:
    global invite_manager
    await invite_manager.bot.wait_until_ready()


@tasks.loop(minutes=config.timing.report_message_interval_minutes)
async def send_report_message() -> None:
    if not messages_enabled:
        return
    channel = bot.get_channel(config.channels.log)
    if channel:
        await channel.send(random.choice(REPORT_TEMPLATES))


@tasks.loop(hours=config.timing.auto_scan_interval_hours)
async def auto_blacklist_scan() -> None:
    if not blacklisted_keywords:
        return
    for guild in bot.guilds:
        log_channel = bot.get_channel(config.channels.auto_scan)
        status_msg = None
        if log_channel:
            status_msg = await log_channel.send("Running scheduled blacklist scan...")
        banned_count, failed_count, matches = await scan_and_ban_blacklisted_members(guild)
        if status_msg:
            try:
                await status_msg.delete()
            except discord.HTTPException:
                pass
        if banned_count > 0 or failed_count > 0:
            if log_channel:
                embed = discord.Embed(title="Scheduled Blacklist Scan Complete", color=discord.Color.red() if banned_count > 0 else discord.Color.orange())
                embed.add_field(name="Members Banned", value=f"**{banned_count}**", inline=True)
                embed.add_field(name="Failed to Ban", value=f"**{failed_count}**", inline=True)
                if matches:
                    match_text = ""
                    for match in matches[:5]:
                        match_text += f"- {match['user'].name} - `{match['keyword']}` in {match['location']}\n"
                    if len(matches) > 5:
                        match_text += f"... and {len(matches) - 5} more"
                    embed.add_field(name="Matches", value=match_text, inline=False)
                embed.set_footer(text=f"Next scan in {config.timing.auto_scan_interval_hours} hours")
                embed.timestamp = datetime.now(timezone.utc)
                result_msg = await log_channel.send(embed=embed)
                await asyncio.sleep(30)
                try:
                    await result_msg.delete()
                except discord.HTTPException:
                    pass


@auto_blacklist_scan.before_loop
async def before_auto_scan() -> None:
    await bot.wait_until_ready()


@tasks.loop(minutes=1)
async def check_giveaways_task() -> None:
    """Check for ended giveaways."""
    if not config.enable_giveaways:
        return
    
    now = datetime.now(timezone.utc)
    to_end = []
    
    for giveaway_id, giveaway in giveaways_data.items():
        if giveaway.get('status') != 'active':
            continue
        try:
            ends_at = datetime.fromisoformat(giveaway['ends_at'])
            if ends_at <= now:
                to_end.append(giveaway_id)
        except:
            pass
    
    for giveaway_id in to_end:
        await end_giveaway(giveaway_id)


async def end_giveaway(giveaway_id: str) -> None:
    """End a giveaway and pick winners."""
    if giveaway_id not in giveaways_data:
        return
    
    giveaway = giveaways_data[giveaway_id]
    entries = giveaway.get('entries', [])
    winner_count = giveaway.get('winner_count', 1)
    
    if entries:
        winners = random.sample(entries, min(winner_count, len(entries)))
        giveaway['winners'] = winners
    else:
        giveaway['winners'] = []
    
    giveaway['status'] = 'ended'
    giveaway['ended_at'] = datetime.now(timezone.utc).isoformat()
    save_giveaways_data()
    
    channel = bot.get_channel(giveaway.get('channel_id'))
    if channel:
        winners = giveaway.get('winners', [])
        if winners:
            winners_mention = " ".join(f"<@{wid}>" for wid in winners)
            embed = EmbedBuilder.success(
                "ðŸŽ‰ Giveaway Ended!",
                f"**Prize:** {giveaway.get('prize', 'Unknown')}\n"
                f"**Winners:** {winners_mention}\n"
                f"**Total Entries:** {len(entries)}"
            )
        else:
            embed = EmbedBuilder.warning(
                "ðŸŽ‰ Giveaway Ended",
                f"**Prize:** {giveaway.get('prize', 'Unknown')}\n"
                f"No valid entries received."
            )
        
        try:
            message = await channel.fetch_message(giveaway.get('message_id', 0))
            await message.edit(embed=embed, view=None)
        except:
            pass
        
        await channel.send(embed=embed)
    
    logging.info(f"[Giveaway] Ended giveaway {giveaway_id}")


@check_giveaways_task.before_loop
async def before_check_giveaways() -> None:
    await bot.wait_until_ready()


@tasks.loop(minutes=30)
async def check_sla_task() -> None:
    """Alert in ticket channel if SLA response time has been breached."""
    if not ticket_tool:
        return
    for guild in bot.guilds:
        settings = data_manager.load_ticket_settings(guild.id)
        sla_hours = settings.get('sla_hours', 0) if settings else 0
        if not sla_hours:
            continue
        open_tickets = data_manager.load_tickets_by_guild(guild.id, 'open')
        for ticket in open_tickets:
            if ticket.get('first_response_at'):
                continue  # Already had a staff response
            try:
                created = datetime.fromisoformat(ticket['created_at'].replace('Z', '+00:00'))
                elapsed_hours = (datetime.now(timezone.utc) - created).total_seconds() / 3600
                if elapsed_hours >= sla_hours:
                    channel = guild.get_channel(ticket['channel_id'])
                    if channel:
                        support_role_id = settings.get('support_role_id')
                        mention = f"<@&{support_role_id}>" if support_role_id else "@here"
                        try:
                            await channel.send(
                                f"⚠️ **SLA Breach** — {mention} This ticket has been open for "
                                f"`{elapsed_hours:.1f}h` with no staff response "
                                f"(SLA: {sla_hours}h). Please respond ASAP.",
                                allowed_mentions=discord.AllowedMentions(roles=True, everyone=True)
                            )
                            # Mark as responded so we don't spam
                            data_manager.update_ticket_first_response(ticket['ticket_id'])
                        except Exception:
                            pass
            except Exception as e:
                logging.warning(f"[SLA] Error checking ticket {ticket.get('ticket_id')}: {e}")


@check_sla_task.before_loop
async def before_check_sla() -> None:
    await bot.wait_until_ready()


# --- UTILITY FUNCTIONS ---
def get_uptime() -> str:
    return str(timedelta(seconds=int(time.time() - start_time)))


def log_event(event_type: str, user: discord.User, details: Optional[str] = None) -> None:
    log_message = f"{event_type} - User: {user} (ID: {user.id})"
    if details:
        log_message += f" | Details: {details}"
    logging.info(log_message)


# --- LEVELING SYSTEM (V2 Enhancement) ---
async def process_leveling(message: discord.Message) -> None:
    if not config.enable_leveling or not message.guild:
        return
    
    user_id = message.author.id
    guild_id = message.guild.id
    key = (user_id, guild_id)
    
    # Check cooldown
    level_data = levels_data.get(key, {'xp': 0, 'level': 0, 'total_messages': 0, 'last_xp_gain': None})
    last_gain = level_data.get('last_xp_gain')
    if last_gain:
        try:
            last_time = datetime.fromisoformat(last_gain)
            if (datetime.now(timezone.utc) - last_time).total_seconds() < 60:
                return
        except:
            pass
    
    # Add XP
    xp_gain = random.randint(5, 15)
    level_data['xp'] = level_data.get('xp', 0) + xp_gain
    level_data['total_messages'] = level_data.get('total_messages', 0) + 1
    level_data['last_xp_gain'] = datetime.now(timezone.utc).isoformat()
    
    # Check for level up
    old_level = level_data.get('level', 0)
    xp = level_data['xp']
    new_level = 0
    while xp >= int((new_level + 1) ** 2.5 * 100):
        new_level += 1
    
    level_data['level'] = new_level
    levels_data[key] = level_data
    
    if new_level > old_level:
        embed = EmbedBuilder.success(
            "ðŸŽ‰ Level Up!",
            f"{message.author.mention} has reached **Level {new_level}**!"
        )
        await message.channel.send(embed=embed, delete_after=10)
    
    # OPTIMIZATION: Save ONLY this user to the database every 10 messages.
    # This prevents the bot from lagging by writing thousands of users at once.
    if level_data['total_messages'] % 10 == 0:
        data_manager.save_level(
            user_id, 
            guild_id, 
            level_data['xp'], 
            level_data['level'], 
            level_data['total_messages'], 
            datetime.fromisoformat(level_data['last_xp_gain']) if level_data.get('last_xp_gain') else None
        )


# --- BOT EVENTS ---
def signal_handler(sig, frame) -> None:
    logging.info("Shutdown signal received. Saving data...")
    save_all_data()
    reset_temporary_data()  # Reset tickets and giveaways
    data_manager.close()  # Close SQLite connection
    process_manager.clear_lock_file()
    logging.info("Data saved. Temporary data reset. Goodbye!")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


@bot.event
async def on_ready() -> None:
    global invite_manager, ticket_tool

    print(f'Logged in as {bot.user.name}')
    print(f'Bot started at: {time.strftime("%Y-%m-%d %H:%M:%S")}')
    logging.info(f'Bot started as {bot.user.name}')

    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name=config.bot_status))

    process_manager.clear_lock_file()

    # Connect to SQLite database first
    data_manager.connect()
    logging.info("[DataManager] SQLite database connected")

    # Import any existing JSON files to SQLite (runs once, then renames files to .bak)
    import_json_to_sqlite()

    # Load all cached data
    send_report_message.start()
    invite_manager = InviteManager(bot)
    invite_manager.load_data()
    load_blacklist_data()
    load_rules_cache()
    load_giveaways_data()
    load_levels_data()

    # Initialize Ticket Tool System
    ticket_tool = TicketToolSystem(data_manager, bot)
    logging.info("[TicketTool] Initialized ticket tool system")

    # Register all existing panel views for persistence
    for guild in bot.guilds:
        guild_panels = data_manager.load_ticket_panels_by_guild(guild.id)
        for panel in guild_panels:
            view = TicketPanelView(panel)
            bot.add_view(view)
    logging.info(f"[TicketTool] Registered views for panels")

    # Clean up orphaned tickets on startup (channels that no longer exist)
    for guild in bot.guilds:
        open_tickets = data_manager.load_tickets_by_guild(guild.id, 'open')
        cleaned = 0
        for ticket in open_tickets:
            if not guild.get_channel(ticket['channel_id']):
                ticket['status'] = 'closed'
                ticket['close_reason'] = 'Orphaned on startup cleanup'
                ticket['closed_at'] = datetime.now(timezone.utc).isoformat()
                data_manager.save_ticket(ticket)
                cleaned += 1
        if cleaned:
            logging.info(f"[TicketTool] Cleaned {cleaned} orphaned ticket(s) in {guild.name}")

    if not check_invites_task.is_running():
        check_invites_task.start()
    logging.info("[InviteManager] Started invite checking task")

    bot.add_view(VerificationButtonsView())

    if not auto_blacklist_scan.is_running():
        auto_blacklist_scan.start()
    logging.info("[Blacklist] Started scheduled auto-scan task")

    if config.enable_giveaways and not check_giveaways_task.is_running():
        check_giveaways_task.start()
        logging.info("[Giveaways] Started giveaway check task")

    if not check_sla_task.is_running():
        check_sla_task.start()
        logging.info("[SLA] Started SLA check task")

    try:
        synced = await bot.tree.sync()
        logging.info(f"[Slash Commands] Synced {len(synced)} slash commands")
        print(f"Synced {len(synced)} slash commands")
    except Exception as e:
        logging.error(f"[Slash Commands] Failed to sync: {e}")

    if FLASK_AVAILABLE:
        threading.Thread(target=run_web, daemon=True).start()


@bot.event
async def on_member_join(member: discord.Member) -> None:
    try:
        banned = await auto_ban_if_blacklisted(member, source="member_join")
        if banned:
            return
        
        channel = bot.get_channel(config.channels.welcome)
        welcome_message = random.choice(WELCOME_TEMPLATES).format(mention=member.mention, server=member.guild.name)
        
        embed = discord.Embed(title=f"Welcome to {member.guild.name}!", description=welcome_message, color=discord.Color.green())
        embed.set_thumbnail(url=member.avatar.url if member.avatar else member.default_avatar.url)
        embed.add_field(name="Member Count", value=member.guild.member_count)
        embed.add_field(name="Account Created", value=member.created_at.strftime("%Y-%m-%d"))
        
        rules_channel = bot.get_channel(config.channels.rules)
        if rules_channel:
            embed.add_field(name="Server Rules", value=f"Make sure to follow Mask Off society's Rules {rules_channel.mention}", inline=False)
        
        embed.set_footer(text=f"Joined on {member.joined_at.strftime('%Y-%m-%d')}")
        
        welcome_msg = await channel.send(embed=embed)
        await welcome_msg.add_reaction('ðŸ”¥')
        await welcome_msg.add_reaction('ðŸ‘‹')
        await welcome_msg.add_reaction('ðŸ’¯')
        
        logging.info(f"Sent welcome message for {member}")
    except Exception as e:
        logging.error(f"Error in welcome system: {str(e)}")


@bot.event
async def on_presence_update(before: discord.Member, after: discord.Member) -> None:
    if after.bot:
        return
    if not blacklisted_keywords:
        return
    
    before_activities = set(str(a) for a in before.activities)
    after_activities = set(str(a) for a in after.activities)
    
    if before_activities == after_activities:
        return
    
    await auto_ban_if_blacklisted(after, source="presence_update")


@bot.event
async def on_message(message: discord.Message) -> None:
    if message.author.bot:
        return

    # Process leveling
    await process_leveling(message)

    # Track first staff response in ticket channels
    if ticket_tool and not message.author.bot:
        ticket = data_manager.load_ticket_by_channel(message.channel.id)
        if ticket and ticket.get('status') == 'open':
            # If the message author is not the ticket creator -> staff response
            if message.author.id != ticket.get('creator_id'):
                data_manager.update_ticket_first_response(ticket['ticket_id'])

    await bot.process_commands(message)


# --- MODERATION COMMANDS ---
@bot.command()
@commands.has_permissions(kick_members=True)
async def kick(ctx: commands.Context, member: discord.Member, *, reason: str = "No reason provided") -> None:
    try:
        await member.kick(reason=reason)
        await ctx.send(embed=EmbedBuilder.success("Member Kicked", f"{member.mention} has been kicked.\n**Reason:** {reason}"))
        logging.info(f'User {member} was kicked by {ctx.author} for: {reason}')
    except discord.Forbidden:
        await ctx.send("I don't have permission to kick that member.")
    except discord.HTTPException:
        await ctx.send("Failed to kick the member. Please try again.")


@bot.command()
@commands.has_permissions(ban_members=True)
async def ban(ctx: commands.Context, member: Optional[discord.Member] = None, *, reason: str = "No reason provided") -> None:
    if member is None:
        await ctx.send("Usage: `!ban @user [reason]` or `!ban <user_id> [reason]`")
        return
    try:
        await member.ban(reason=reason)
        await ctx.send(embed=EmbedBuilder.success("Member Banned", f"{member.mention} has been banned.\n**Reason:** {reason}"))
        logging.info(f'User {member} was banned by {ctx.author} for: {reason}')
    except discord.Forbidden:
        await ctx.send("I don't have permission to ban that user.")
    except discord.HTTPException:
        await ctx.send("Failed to ban the user. Please try again.")

@bot.command()
@commands.has_permissions(ban_members=True)
async def banid(ctx: commands.Context, user_id: int, *, reason: str = "No reason provided") -> None:
    try:
        user = await bot.fetch_user(user_id)
        await ctx.guild.ban(user, reason=reason)
        await ctx.send(embed=EmbedBuilder.success("User Banned", f"**{user.name}** (ID: {user_id}) has been banned.\n**Reason:** {reason}"))
        logging.info(f'User with ID {user_id} was banned by {ctx.author} for: {reason}')
    except discord.NotFound:
        await ctx.send(f'User with ID {user_id} not found.')
    except discord.Forbidden:
        await ctx.send("I don't have permission to ban that user.")
    except discord.HTTPException:
        await ctx.send("Failed to ban the user. Please try again.")


# --- BLACKLIST COMMANDS ---
@bot.hybrid_command(name="blacklist", description="Add a keyword to the blacklist")
@commands.has_permissions(administrator=True)
@app_commands.describe(keyword="The keyword to blacklist")
async def blacklist_cmd(ctx: commands.Context, *, keyword: str) -> None:
    global blacklisted_keywords
    
    keyword = keyword.strip()
    if not keyword:
        await ctx.send("Please provide a keyword to blacklist.")
        return
    
    if len(keyword) < config.limits.min_blacklist_keyword_length:
        await ctx.send(f"Keyword must be at least {config.limits.min_blacklist_keyword_length} characters long.")
        return
    
    for existing in blacklisted_keywords:
        if existing.lower() == keyword.lower():
            await ctx.send(f"Keyword `{keyword}` is already blacklisted.")
            return
    
    blacklisted_keywords.add(keyword)
    save_blacklist_data()
    
    embed = EmbedBuilder.warning(
        "Keyword Blacklisted",
        f"Added `{keyword}` to blacklist.\nTotal keywords: {len(blacklisted_keywords)}\n\n"
        f"Any user with this keyword in their profile will be auto-banned."
    )
    
    await ctx.send(embed=embed)
    logging.info(f"[Blacklist] Keyword '{keyword}' added by {ctx.author}")


@bot.hybrid_command(name="unblacklist", description="Remove a keyword from the blacklist")
@commands.has_permissions(administrator=True)
@app_commands.describe(keyword="The keyword to remove from the blacklist")
@app_commands.autocomplete(keyword=blacklist_autocomplete)
async def unblacklist_cmd(ctx: commands.Context, *, keyword: str) -> None:
    global blacklisted_keywords
    
    keyword = keyword.strip()
    found_keyword = next((k for k in blacklisted_keywords if k.lower() == keyword.lower()), None)
    
    if not found_keyword:
        await ctx.send(f"Keyword `{keyword}` is not in the blacklist.")
        return
    
    blacklisted_keywords.discard(found_keyword)
    save_blacklist_data()
    
    await ctx.send(embed=EmbedBuilder.success("Keyword Removed", f"Removed `{found_keyword}` from blacklist."))
    logging.info(f"[Blacklist] Keyword '{found_keyword}' removed by {ctx.author}")


@bot.hybrid_command(name="blacklistscan", description="Scan all members for blacklisted keywords")
@commands.has_permissions(administrator=True)
async def blacklistscan_cmd(ctx: commands.Context) -> None:
    if not blacklisted_keywords:
        await ctx.send("No keywords are currently blacklisted. Use `/blacklist <keyword>` to add some.")
        return
    
    status_msg = await ctx.send(f"Scanning **{ctx.guild.member_count}** members for **{len(blacklisted_keywords)}** blacklisted keyword(s)...")
    
    banned_count, failed_count, matches = await scan_and_ban_blacklisted_members(ctx.guild)
    await status_msg.delete()
    
    embed = EmbedBuilder.warning(
        "Blacklist Scan Complete",
        f"**Members Scanned:** {ctx.guild.member_count}\n"
        f"**Members Banned:** {banned_count}\n"
        f"**Failed to Ban:** {failed_count}"
    )
    
    if matches:
        match_text = ""
        for match in matches[:5]:
            match_text += f"- {match['user'].name} - \"{match['keyword']}\" in {match['location']}\n"
        if len(matches) > 5:
            match_text += f"... and {len(matches) - 5} more"
        embed.add_field(name="Matches Found", value=match_text, inline=False)
    
    await ctx.send(embed=embed)
    logging.info(f"[Blacklist] Scan complete by {ctx.author}. Banned: {banned_count}, Failed: {failed_count}")


@bot.hybrid_command(name="blacklistlist", description="Display all blacklisted keywords")
@commands.has_permissions(administrator=True)
async def blacklistlist_cmd(ctx: commands.Context) -> None:
    if not blacklisted_keywords:
        await ctx.send("**No keywords are currently blacklisted.**")
        return
    
    keywords_list = list(blacklisted_keywords)
    embed = discord.Embed(title="Blacklisted Keywords", color=discord.Color.red())
    
    chunks = [keywords_list[i:i+20] for i in range(0, len(keywords_list), 20)]
    for i, chunk in enumerate(chunks[:5]):
        field_name = "Keywords" if i == 0 else "Keywords (continued)"
        embed.add_field(name=field_name, value="\n".join(f"- `{kw}`" for kw in chunk), inline=False)
    
    if len(chunks) > 5:
        embed.add_field(name="...", value=f"And {len(keywords_list) - 100} more keywords", inline=False)
    
    embed.set_footer(text=f"Total: {len(blacklisted_keywords)} keyword(s)")
    await ctx.send(embed=embed)


@bot.hybrid_command(name="checkprofile", description="Check a user's profile for blacklisted keywords")
@app_commands.describe(member="The member to check")
async def checkprofile_cmd(ctx: commands.Context, member: Optional[discord.Member] = None) -> None:
    if member is None:
        member = ctx.author
    
    is_blacklisted, keyword, location = await check_user_profile_for_blacklist(member)
    
    embed = discord.Embed(title=f"Profile Check: {member.display_name}", color=discord.Color.red() if is_blacklisted else discord.Color.green())
    embed.set_thumbnail(url=member.avatar.url if member.avatar else member.default_avatar.url)
    
    if is_blacklisted:
        embed.add_field(name="Status", value="**BLACKLISTED**", inline=True)
        embed.add_field(name="Matched Keyword", value=f"**{keyword}**", inline=True)
        embed.add_field(name="Location", value=location, inline=True)
    else:
        embed.add_field(name="Status", value="**Clean**", inline=True)
        embed.add_field(name="Keywords Checked", value=str(len(blacklisted_keywords)), inline=True)
    
    await ctx.send(embed=embed)


# --- RULES MANAGEMENT COMMANDS ---
@bot.hybrid_command(name="updatemosrules", description="Manually update the cached MOS rules")
@commands.has_permissions(administrator=True)
async def updatemosrules_cmd(ctx: commands.Context) -> None:
    try:
        channel = bot.get_channel(config.servers.mos_rules_channel)
        if not channel:
            await ctx.send("MOS rules channel not found.")
            return
        message = await channel.fetch_message(config.servers.mos_rules_message)
        rules_cache['mos_rules'] = message.content
        rules_cache['mos_last_updated'] = datetime.now(timezone.utc).isoformat()
        save_rules_cache()
        
        embed = EmbedBuilder.success("MOS Rules Updated", f"Successfully cached the MOS rules.\n**Characters:** {len(message.content)}")
        await ctx.send(embed=embed)
        logging.info(f"[RulesCache] MOS rules manually updated by {ctx.author}")
    except discord.NotFound:
        await ctx.send("MOS rules message not found.")
    except discord.Forbidden:
        await ctx.send("No permission to fetch the MOS rules message.")
    except Exception as e:
        await ctx.send(f"Error: {str(e)}")


@bot.hybrid_command(name="updatevprprules", description="Manually update the cached VPRP rules")
@commands.has_permissions(administrator=True)
async def updatevprprules_cmd(ctx: commands.Context) -> None:
    try:
        vprp_guild = bot.get_guild(config.servers.vprp_server_id)
        if not vprp_guild:
            await ctx.send("Bot is not in the VPRP server. Cannot fetch rules.\nUse `/setvprprules` to manually set the rules.")
            return
        channel = vprp_guild.get_channel(config.servers.vprp_rules_channel)
        if not channel:
            await ctx.send("VPRP rules channel not found.")
            return
        message = await channel.fetch_message(config.servers.vprp_rules_message)
        rules_cache['vprp_rules'] = message.content
        rules_cache['vprp_last_updated'] = datetime.now(timezone.utc).isoformat()
        save_rules_cache()
        
        embed = EmbedBuilder.success("VPRP Rules Updated", f"Successfully cached the VPRP rules.\n**Characters:** {len(message.content)}")
        await ctx.send(embed=embed)
        logging.info(f"[RulesCache] VPRP rules manually updated by {ctx.author}")
    except discord.NotFound:
        await ctx.send("VPRP rules message not found.")
    except discord.Forbidden:
        await ctx.send("No permission to fetch the VPRP rules message.")
    except Exception as e:
        await ctx.send(f"Error: {str(e)}")


@bot.hybrid_command(name="setvprprules", description="Manually set the VPRP rules text")
@commands.has_permissions(administrator=True)
async def setvprprules_cmd(ctx: commands.Context, *, rules: str) -> None:
    rules_cache['vprp_rules'] = rules
    rules_cache['vprp_last_updated'] = datetime.now(timezone.utc).isoformat()
    save_rules_cache()
    
    embed = EmbedBuilder.success("VPRP Rules Set", f"Successfully saved the VPRP rules.\n**Characters:** {len(rules)}")
    await ctx.send(embed=embed)
    logging.info(f"[RulesCache] VPRP rules manually set by {ctx.author}")


@bot.hybrid_command(name="setmosrules", description="Manually set the MOS rules text")
@commands.has_permissions(administrator=True)
async def setmosrules_cmd(ctx: commands.Context, *, rules: str) -> None:
    rules_cache['mos_rules'] = rules
    rules_cache['mos_last_updated'] = datetime.now(timezone.utc).isoformat()
    save_rules_cache()
    
    embed = EmbedBuilder.success("MOS Rules Set", f"Successfully saved the MOS rules.\n**Characters:** {len(rules)}")
    await ctx.send(embed=embed)
    logging.info(f"[RulesCache] MOS rules manually set by {ctx.author}")


@bot.hybrid_command(name="viewcachedrules", description="View the currently cached rules")
@commands.has_permissions(administrator=True)
async def viewcachedrules_cmd(ctx: commands.Context) -> None:
    embed = discord.Embed(title="Cached Rules Status", color=discord.Color.blurple())
    
    mos_status = f"Cached ({len(rules_cache['mos_rules'])} chars)" if rules_cache['mos_rules'] else "Not cached"
    vprp_status = f"Cached ({len(rules_cache['vprp_rules'])} chars)" if rules_cache['vprp_rules'] else "Not cached"
    
    embed.add_field(name="MOS Rules", value=f"{mos_status}\nLast Updated: {rules_cache['mos_last_updated'][:19] if rules_cache['mos_last_updated'] else 'Never'}", inline=True)
    embed.add_field(name="VPRP Rules", value=f"{vprp_status}\nLast Updated: {rules_cache['vprp_last_updated'][:19] if rules_cache['vprp_last_updated'] else 'Never'}", inline=True)
    
    await ctx.send(embed=embed)


# --- PURGE / CHANNEL COMMANDS ---
@bot.command()
@commands.has_permissions(manage_messages=True)
async def purgeall(ctx: commands.Context) -> None:
    deleted_messages = 0
    while True:
        try:
            batch = await ctx.channel.purge(limit=100)
            deleted_messages += len(batch)
            if len(batch) < 100:
                break
            await asyncio.sleep(1)
        except discord.HTTPException as e:
            if e.status == 429:
                retry_after = int(e.response.headers.get('Retry-After', 1)) if hasattr(e.response, 'headers') else 1
                await asyncio.sleep(retry_after)
            else:
                break
    
    message = await ctx.send(f'Deleted {deleted_messages} messages.')
    await asyncio.sleep(2)
    await message.delete()
    logging.info(f'{ctx.author} purged all messages in {ctx.channel}')


@bot.command()
@commands.has_permissions(manage_messages=True)
async def purge(ctx: commands.Context, amount: int) -> None:
    deleted_messages = await ctx.channel.purge(limit=amount + 1)
    message = await ctx.send(f'Deleted {len(deleted_messages)} messages.')
    await asyncio.sleep(2)
    await message.delete()
    logging.info(f'{ctx.author} purged {len(deleted_messages)} messages in {ctx.channel}')


@bot.command()
@commands.has_permissions(manage_roles=True)
async def mute(ctx: commands.Context, member: discord.Member, *, reason: Optional[str] = None) -> None:
    if config.roles.staff in [role.id for role in member.roles]:
        await ctx.send(f'{member.mention} cannot be muted because they have the Staff role.')
        return
    
    mute_role = discord.utils.get(ctx.guild.roles, name='Muted')
    if not mute_role:
        message = await ctx.send('Mute role not found. Please create a role named "Muted".')
        await asyncio.sleep(2)
        await message.delete()
        return
    
    try:
        await member.add_roles(mute_role)
        response = f'Muted {member.mention} for: {reason}' if reason else f'Muted {member.mention} without a specified reason.'
        await ctx.send(embed=EmbedBuilder.warning("Member Muted", response))
        logging.info(f'User {member} was muted by {ctx.author} for: {reason}')
    except discord.Forbidden:
        await ctx.send("I do not have permission to mute that member.")
    except discord.HTTPException:
        await ctx.send("Failed to mute the member. Please try again.")


@bot.command()
@commands.has_permissions(manage_roles=True)
async def unmute(ctx: commands.Context, member: discord.Member) -> None:
    mute_role = discord.utils.get(ctx.guild.roles, name='Muted')
    if mute_role:
        await member.remove_roles(mute_role)
        await ctx.send(embed=EmbedBuilder.success("Member Unmuted", f"{member.mention} has been unmuted."))
        logging.info(f'User {member} was unmuted by {ctx.author}')
    else:
        await ctx.send('Mute role not found.')


@bot.command()
@commands.has_permissions(manage_channels=True)
async def lock(ctx: commands.Context) -> None:
    await ctx.channel.set_permissions(ctx.guild.default_role, send_messages=False)
    await ctx.send(embed=EmbedBuilder.warning("Channel Locked", "This channel is now locked."))
    logging.info(f'Channel {ctx.channel} was locked by {ctx.author}')


@bot.command()
@commands.has_permissions(manage_channels=True)
async def unlock(ctx: commands.Context) -> None:
    await ctx.channel.set_permissions(ctx.guild.default_role, send_messages=True)
    await ctx.send(embed=EmbedBuilder.success("Channel Unlocked", "This channel is now unlocked."))
    logging.info(f'Channel {ctx.channel} was unlocked by {ctx.author}')


@bot.command()
@commands.has_permissions(manage_channels=True)
async def slowmode(ctx: commands.Context, seconds: int) -> None:
    await ctx.channel.edit(slowmode_delay=seconds)
    await ctx.send(embed=EmbedBuilder.info("Slowmode Set", f"Slowmode set to {seconds} seconds."))
    logging.info(f'Slowmode in {ctx.channel} was set to {seconds}s by {ctx.author}')


@bot.command()
@commands.has_permissions(manage_roles=True)
async def addrole(ctx: commands.Context, member: discord.Member, *, role_name: str) -> None:
    role = discord.utils.get(ctx.guild.roles, name=role_name)
    if role is None:
        await ctx.send(f'Role "{role_name}" not found.')
        return
    if role in member.roles:
        await ctx.send(f"{member.mention} already has the {role_name} role.")
        return
    await member.add_roles(role)
    await ctx.send(embed=EmbedBuilder.success("Role Added", f"Added **{role_name}** to {member.mention}."))
    log_event("Role Added", ctx.author, f"Added {role_name} to {member}")


@bot.command()
@commands.has_permissions(manage_roles=True)
async def roleall(ctx: commands.Context, role: discord.Role) -> None:
    if role is None:
        await ctx.send("Please mention a valid role.")
        return
    
    members_assigned = 0
    failed_members = 0
    
    for member in ctx.guild.members:
        if member.bot:
            continue
        if role not in member.roles:
            try:
                await member.add_roles(role)
                members_assigned += 1
            except (discord.Forbidden, discord.HTTPException):
                failed_members += 1
    
    await ctx.send(embed=EmbedBuilder.success("Role Assigned", f"Assigned {role.mention} to **{members_assigned}** members.{f' Failed: {failed_members}' if failed_members else ''}"))
    log_event("Role All Assigned", ctx.author, f"Assigned {role.name} to {members_assigned} members")


@bot.command()
@commands.has_permissions(manage_roles=True)
async def removerole(ctx: commands.Context, member: discord.Member, role: discord.Role) -> None:
    try:
        await member.remove_roles(role)
        await ctx.send(embed=EmbedBuilder.success("Role Removed", f"Removed **{role.name}** from {member.mention}."))
        logging.info(f'Role {role.name} was removed from {member} by {ctx.author}')
    except discord.Forbidden:
        await ctx.send("I do not have permission to remove that role.")
    except discord.HTTPException:
        await ctx.send("Failed to remove the role. Please try again.")


@bot.command()
@commands.has_permissions(ban_members=True)
async def softban(ctx: commands.Context, member: discord.Member, *, reason: Optional[str] = None) -> None:
    await member.ban(reason=reason)
    await ctx.guild.unban(member)
    await ctx.send(embed=EmbedBuilder.warning("Member Softbanned", f"{member.mention} has been softbanned.\n**Reason:** {reason or 'No reason provided'}"))
    logging.info(f'User {member} was softbanned by {ctx.author} for: {reason}')


@bot.command()
@commands.has_permissions(manage_roles=True)
async def tempmute(ctx: commands.Context, member: discord.Member, duration: int, *, reason: Optional[str] = None) -> None:
    mute_role = discord.utils.get(ctx.guild.roles, name='Muted')
    if not mute_role:
        await ctx.send("Mute role not found. Please create a role named 'Muted'.")
        return
    
    try:
        await member.add_roles(mute_role, reason=reason)
        await ctx.send(embed=EmbedBuilder.warning("Member Temp-Muted", f"{member.mention} muted for {duration} seconds.\n**Reason:** {reason or 'No reason provided'}"))
        logging.info(f'User {member} was temp-muted by {ctx.author} for {duration}s')
        
        await asyncio.sleep(duration)
        
        if member in ctx.guild.members:
            await member.remove_roles(mute_role)
            await ctx.send(f'Unmuted {member.mention} after {duration} seconds.')
    except discord.Forbidden:
        await ctx.send("I don't have permission to manage roles for that member.")
    except discord.HTTPException:
        await ctx.send("Failed to mute the member. Please try again.")


# --- WARNING COMMANDS (V2 Enhancement) ---
@bot.hybrid_command(name="warn", description="Warn a member")
@commands.has_permissions(manage_roles=True)
@app_commands.describe(member="Member to warn", reason="Reason for warning")
async def warn_cmd(ctx: commands.Context, member: discord.Member, *, reason: str) -> None:
    if not config.enable_warnings:
        await ctx.send("Warning system is disabled.")
        return
    
    import uuid
    warning_id = str(uuid.uuid4())[:8]
    
    warning = {
        'warning_id': warning_id,
        'user_id': member.id,
        'guild_id': ctx.guild.id,
        'moderator_id': ctx.author.id,
        'reason': reason,
        'points': 1,
        'created_at': datetime.now(timezone.utc).isoformat(),
        'is_active': True
    }
    
    if ctx.guild.id not in warnings_data:
        warnings_data[ctx.guild.id] = []
    warnings_data[ctx.guild.id].append(warning)
    
    # Count total active warnings
    total_points = sum(1 for w in warnings_data[ctx.guild.id] if w['user_id'] == member.id and w['is_active'])
    
    embed = EmbedBuilder.warning(
        "Member Warned",
        f"{member.mention} has been warned.\n"
        f"**Reason:** {reason}\n"
        f"**Warning ID:** {warning_id}\n"
        f"**Total Points:** {total_points}/{config.limits.max_warnings_before_ban}"
    )
    
    await ctx.send(embed=embed)
    
    # Auto-ban if exceeded
    if total_points >= config.limits.max_warnings_before_ban:
        try:
            await member.ban(reason=f"Exceeded warning limit ({total_points} points)")
            await ctx.send(embed=EmbedBuilder.error("Auto-Ban", f"{member.mention} has been auto-banned for exceeding warning limit."))
        except:
            pass
    
    logging.info(f"[Warnings] {member} warned by {ctx.author}: {reason}")


@bot.hybrid_command(name="warnings", description="View warnings for a member")
@app_commands.describe(member="Member to check")
async def warnings_cmd(ctx: commands.Context, member: Optional[discord.Member] = None) -> None:
    member = member or ctx.author
    
    guild_warnings = warnings_data.get(ctx.guild.id, [])
    user_warnings = [w for w in guild_warnings if w['user_id'] == member.id and w['is_active']]
    
    if not user_warnings:
        await ctx.send(embed=EmbedBuilder.info("Warnings", f"{member.mention} has no active warnings."))
        return
    
    embed = EmbedBuilder.warning(f"Warnings for {member.display_name}", f"Total Active: {len(user_warnings)}")
    
    for w in user_warnings[:5]:
        created = datetime.fromisoformat(w['created_at']).strftime('%Y-%m-%d')
        embed.add_field(
            name=f"Warning {w['warning_id']}",
            value=f"**Reason:** {w['reason']}\n**By:** <@{w['moderator_id']}>\n**Date:** {created}",
            inline=False
        )
    
    await ctx.send(embed=embed)


@bot.hybrid_command(name="clearwarnings", description="Clear warnings for a member")
@commands.has_permissions(administrator=True)
@app_commands.describe(member="Member to clear warnings for")
async def clearwarnings_cmd(ctx: commands.Context, member: discord.Member) -> None:
    global warnings_data
    
    if ctx.guild.id not in warnings_data:
        await ctx.send(f"{member.mention} has no warnings.")
        return
    
    count = 0
    for w in warnings_data[ctx.guild.id]:
        if w['user_id'] == member.id and w['is_active']:
            w['is_active'] = False
            count += 1
    
    await ctx.send(embed=EmbedBuilder.success("Warnings Cleared", f"Cleared {count} warning(s) for {member.mention}."))
    logging.info(f"[Warnings] {ctx.author} cleared {count} warnings for {member}")


# =============================================================================
# --- TICKET TOOL COMMANDS (Full Ticket Tool Clone) ---
@bot.hybrid_command(name="panel", description="Create a new ticket panel")
@commands.has_permissions(manage_channels=True)
async def create_panel(ctx: commands.Context) -> None:
    """Open the interactive panel creator."""
    view = PanelCreatorView(ctx.guild.id, ctx.author.id)
    embed = discord.Embed(
        title="Panel Creator",
        description="Use the buttons below to configure your ticket panel.\n\n"
                    "**Steps:**\n"
                    "1. Set Name - Give your panel a name\n"
                    "2. Set Embed - Customize the embed appearance\n"
                    "3. Set Button - Customize the create button\n"
                    "4. Settings - Configure ticket limits and more\n"
                    "5. Preview - See how it will look\n"
                    "6. Create Panel - Send the panel to this channel",
        color=discord.Color.blurple()
    )
    await ctx.send(embed=embed, view=view)


@bot.hybrid_command(name="panels", description="List all ticket panels")
@commands.has_permissions(manage_channels=True)
async def list_panels(ctx: commands.Context) -> None:
    """List all ticket panels in this server."""
    panels = data_manager.load_ticket_panels_by_guild(ctx.guild.id)
    
    if not panels:
        await ctx.send("No ticket panels found. Use `/panel` to create one.")
        return
    
    embed = discord.Embed(
        title="Ticket Panels",
        description=f"Found **{len(panels)}** panel(s) in this server:",
        color=discord.Color.blurple()
    )
    
    for panel in panels[:10]:
        channel = ctx.guild.get_channel(panel.get('channel_id'))
        channel_name = channel.mention if channel else "Unknown"
        embed.add_field(
            name=f"{panel.get('name', 'Unnamed')} (ID: {panel['panel_id']})",
            value=f"Channel: {channel_name}\nButton: {panel.get('button_label', 'Create Ticket')}",
            inline=False
        )
    
    await ctx.send(embed=embed)


@bot.hybrid_command(name="deletepanel", description="Delete a ticket panel")
@commands.has_permissions(manage_channels=True)
@app_commands.describe(panel_id="The panel ID to delete")
async def delete_panel(ctx: commands.Context, panel_id: str) -> None:
    """Delete a ticket panel."""
    panel = data_manager.load_ticket_panel(panel_id)
    
    if not panel or panel['guild_id'] != ctx.guild.id:
        await ctx.send("Panel not found in this server.")
        return
    
    if panel.get('channel_id') and panel.get('message_id'):
        try:
            channel = ctx.guild.get_channel(panel['channel_id'])
            if channel:
                message = await channel.fetch_message(panel['message_id'])
                await message.delete()
        except:
            pass
    
    data_manager.delete_ticket_panel(panel_id)
    await ctx.send(f"Panel `{panel_id}` has been deleted.")


@bot.hybrid_command(name="claim", description="Claim the current ticket")
async def claim_ticket_cmd(ctx: commands.Context) -> None:
    """Claim a ticket."""
    if not ticket_tool:
        await ctx.send("Ticket system not initialized.")
        return
    
    success, message = await ticket_tool.claim_ticket(ctx.channel, ctx.author)
    
    if success:
        await ctx.send(embed=discord.Embed(title="Ticket Claimed", description=message, color=discord.Color.green()))
    else:
        await ctx.send(message)


@bot.hybrid_command(name="unclaim", description="Release your claim on this ticket")
async def unclaim_ticket_cmd(ctx: commands.Context) -> None:
    """Release a ticket claim."""
    if not ticket_tool:
        await ctx.send("Ticket system not initialized.")
        return
    
    success, message = await ticket_tool.unclaim_ticket(ctx.channel, ctx.author)
    
    if success:
        await ctx.send(embed=discord.Embed(title="Ticket Unclaimed", description=message, color=discord.Color.orange()))
    else:
        await ctx.send(message)


@bot.hybrid_command(name="close", description="Close the current ticket")
@app_commands.describe(reason="Reason for closing")
async def close_ticket_cmd(ctx: commands.Context, *, reason: str = "No reason provided") -> None:
    """Close a ticket with optional reason."""
    if not ticket_tool:
        await ctx.send("Ticket system not initialized.")
        return
    
    ticket = data_manager.load_ticket_by_channel(ctx.channel.id)
    if not ticket:
        await ctx.send("This is not a ticket channel.")
        return
    
    # TicketRatingView handles closing after rating (or on timeout)
    view = TicketRatingView(ticket['ticket_id'], reason, ctx.channel, ctx.author)
    await ctx.send(
        "Please rate your support experience before the ticket closes (or skip to close immediately):",
        view=view
    )


@bot.hybrid_command(name="transcript", description="Generate a transcript of this ticket")
async def transcript_cmd(ctx: commands.Context) -> None:
    """Generate a transcript of the current ticket."""
    if not ticket_tool:
        await ctx.send("Ticket system not initialized.")
        return
    
    ticket = data_manager.load_ticket_by_channel(ctx.channel.id)
    if not ticket:
        await ctx.send("This is not a ticket channel.")
        return
    
    transcript = await ticket_tool._generate_transcript(ctx.channel, ticket, ctx.author)
    await ctx.send(embed=transcript['embed'], file=transcript['file'])


@bot.hybrid_command(name="ticketsettings", description="Configure ticket system settings")
@commands.has_permissions(manage_guild=True)
async def ticket_settings_cmd(ctx: commands.Context) -> None:
    """Open ticket settings configuration."""
    settings = data_manager.load_ticket_settings(ctx.guild.id) or {}
    
    embed = discord.Embed(title="Ticket System Settings", color=discord.Color.blurple())
    
    category_id = settings.get('category_id')
    category = ctx.guild.get_channel(category_id) if category_id else None
    
    transcripts_id = settings.get('transcripts_channel_id')
    transcripts = ctx.guild.get_channel(transcripts_id) if transcripts_id else None
    
    support_id = settings.get('support_role_id')
    support_role = ctx.guild.get_role(support_id) if support_id else None
    
    embed.add_field(name="Ticket Category", value=category.name if category else "Not Set", inline=True)
    embed.add_field(name="Transcripts Channel", value=transcripts.mention if transcripts else "Not Set", inline=True)
    embed.add_field(name="Support Role", value=support_role.mention if support_role else "Not Set", inline=True)
    embed.add_field(name="Max Tickets/User", value=str(settings.get('max_tickets_per_user', 3)), inline=True)
    embed.add_field(name="Auto-Close Hours", value=str(settings.get('auto_close_hours', 24)), inline=True)
    embed.add_field(name="DM Transcripts", value="Yes" if settings.get('dm_transcripts', 1) else "No", inline=True)
    
    embed.set_footer(text="Use the modal to update settings")
    
    await ctx.send(embed=embed, view=TicketSettingsConfigView(ctx.guild.id))


class TicketSettingsConfigView(View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=300)
        self.guild_id = guild_id
    
    @discord.ui.button(label="Set Category", style=discord.ButtonStyle.primary)
    async def set_category(self, interaction: discord.Interaction, button: Button) -> None:
        await interaction.response.send_modal(SetTicketCategoryModal(self.guild_id))
    
    @discord.ui.button(label="Set Transcripts", style=discord.ButtonStyle.primary)
    async def set_transcripts(self, interaction: discord.Interaction, button: Button) -> None:
        await interaction.response.send_modal(SetTranscriptsChannelModal(self.guild_id))
    
    @discord.ui.button(label="Set Support Role", style=discord.ButtonStyle.secondary)
    async def set_support_role(self, interaction: discord.Interaction, button: Button) -> None:
        await interaction.response.send_modal(SetSupportRoleModal(self.guild_id))


class SetTicketCategoryModal(Modal, title="Set Ticket Category"):
    category_input = TextInput(label="Category ID", placeholder="Enter category channel ID")
    
    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id
        settings = data_manager.load_ticket_settings(guild_id) or {}
        if settings.get('category_id'):
            self.category_input.default = str(settings['category_id'])
    
    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            category_id = int(self.category_input.value)
            category = interaction.guild.get_channel(category_id)
            if not category or not isinstance(category, discord.CategoryChannel):
                await interaction.response.send_message("Invalid category ID.", ephemeral=True)
                return
            settings = data_manager.load_ticket_settings(self.guild_id) or {'guild_id': self.guild_id}
            settings['category_id'] = category_id
            settings['updated_at'] = datetime.now(timezone.utc).isoformat()
            data_manager.save_ticket_settings(settings)
            await interaction.response.send_message(f"Ticket category set to **{category.name}**", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Please enter a valid number.", ephemeral=True)


class SetTranscriptsChannelModal(Modal, title="Set Transcripts Channel"):
    channel_input = TextInput(label="Channel ID", placeholder="Enter transcripts channel ID")
    
    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id
        settings = data_manager.load_ticket_settings(guild_id) or {}
        if settings.get('transcripts_channel_id'):
            self.channel_input.default = str(settings['transcripts_channel_id'])
    
    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            channel_id = int(self.channel_input.value)
            channel = interaction.guild.get_channel(channel_id)
            if not channel or not isinstance(channel, discord.TextChannel):
                await interaction.response.send_message("Invalid channel ID.", ephemeral=True)
                return
            settings = data_manager.load_ticket_settings(self.guild_id) or {'guild_id': self.guild_id}
            settings['transcripts_channel_id'] = channel_id
            settings['updated_at'] = datetime.now(timezone.utc).isoformat()
            data_manager.save_ticket_settings(settings)
            await interaction.response.send_message(f"Transcripts channel set to {channel.mention}", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Please enter a valid number.", ephemeral=True)


class SetSupportRoleModal(Modal, title="Set Support Role"):
    role_input = TextInput(label="Role ID", placeholder="Enter support role ID")
    
    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id
        settings = data_manager.load_ticket_settings(guild_id) or {}
        if settings.get('support_role_id'):
            self.role_input.default = str(settings['support_role_id'])
    
    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            role_id = int(self.role_input.value)
            role = interaction.guild.get_role(role_id)
            if not role:
                await interaction.response.send_message("Invalid role ID.", ephemeral=True)
                return
            settings = data_manager.load_ticket_settings(self.guild_id) or {'guild_id': self.guild_id}
            settings['support_role_id'] = role_id
            settings['updated_at'] = datetime.now(timezone.utc).isoformat()
            data_manager.save_ticket_settings(settings)
            await interaction.response.send_message(f"Support role set to {role.mention}", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Please enter a valid number.", ephemeral=True)


@bot.hybrid_command(name="ticketblacklist", description="Blacklist a user from creating tickets")
@commands.has_permissions(manage_guild=True)
@app_commands.describe(user="User to blacklist", reason="Reason for blacklist")
async def ticket_blacklist_cmd(ctx: commands.Context, user: discord.Member, *, reason: str = "No reason provided") -> None:
    """Blacklist a user from creating tickets."""
    import uuid
    blacklist_data = {
        'blacklist_id': str(uuid.uuid4())[:8],
        'guild_id': ctx.guild.id,
        'user_id': user.id,
        'reason': reason,
        'blacklisted_by': ctx.author.id,
        'blacklisted_at': datetime.now(timezone.utc).isoformat(),
        'is_active': 1
    }
    data_manager.save_ticket_blacklist(blacklist_data)
    await ctx.send(embed=discord.Embed(
        title="User Blacklisted",
        description=f"{user.mention} has been blacklisted from creating tickets.\n**Reason:** {reason}",
        color=discord.Color.red()
    ))


@bot.hybrid_command(name="ticketunblacklist", description="Remove a user from the ticket blacklist")
@commands.has_permissions(manage_guild=True)
@app_commands.describe(user="User to unblacklist")
async def ticket_unblacklist_cmd(ctx: commands.Context, user: discord.Member) -> None:
    """Remove a user from the ticket blacklist."""
    success = data_manager.remove_ticket_blacklist(ctx.guild.id, user.id)
    if success:
        await ctx.send(f"{user.mention} has been removed from the blacklist.")
    else:
        await ctx.send(f"{user.mention} is not blacklisted.")


@bot.hybrid_command(name="tickets", description="View open tickets")
@commands.has_permissions(manage_channels=True)
async def view_tickets_cmd(ctx: commands.Context) -> None:
    tickets = data_manager.load_tickets_by_guild(ctx.guild.id, 'open')
    if not tickets:
        await ctx.send("No open tickets.")
        return

    # Filter out orphaned tickets (channel no longer exists)
    valid_tickets = []
    for ticket in tickets:
        channel = ctx.guild.get_channel(ticket['channel_id'])
        if channel:
            valid_tickets.append(ticket)
        else:
            # Auto-close orphaned tickets
            ticket['status'] = 'closed'
            ticket['close_reason'] = 'Channel no longer exists (auto-cleaned)'
            ticket['closed_at'] = datetime.now(timezone.utc).isoformat()
            data_manager.save_ticket(ticket)

    if not valid_tickets:
        await ctx.send("No open tickets with active channels.")
        return

    embed = discord.Embed(title="Open Tickets", description=f"Found **{len(valid_tickets)}** open ticket(s)", color=discord.Color.blurple())
    for ticket in valid_tickets[:10]:
        creator = ctx.guild.get_member(ticket['creator_id'])
        creator_name = creator.mention if creator else f"<@{ticket['creator_id']}>"
        claimed = ""
        if ticket.get('claimed_by'):
            claimer = ctx.guild.get_member(ticket['claimed_by'])
            claimed = f"\nClaimed: {claimer.mention if claimer else 'Unknown'}"
        embed.add_field(
            name=f"Ticket #{ticket['ticket_id']}",
            value=f"Creator: {creator_name}\nCategory: {ticket.get('category', 'General')}\nChannel: <#{ticket['channel_id']}>{claimed}",
            inline=False
        )
    await ctx.send(embed=embed)


# =============================================================================
# --- NEW TICKET TOOL FEATURE COMMANDS ---
# =============================================================================

@bot.hybrid_command(name="add", description="Add a user to the current ticket")
@commands.has_permissions(manage_channels=True)
@app_commands.describe(user="User to add to this ticket")
async def ticket_add_cmd(ctx: commands.Context, user: discord.Member) -> None:
    ticket = data_manager.load_ticket_by_channel(ctx.channel.id)
    if not ticket:
        await ctx.send("This is not a ticket channel.", ephemeral=True)
        return
    await ctx.channel.set_permissions(
        user,
        view_channel=True,
        send_messages=True,
        read_message_history=True,
        attach_files=True
    )
    await ctx.send(embed=discord.Embed(
        description=f"✅ {user.mention} has been added to the ticket.",
        color=discord.Color.green()
    ))
    logging.info(f"[Tickets] {ctx.author} added {user} to ticket {ticket['ticket_id']}")


@bot.hybrid_command(name="remove", description="Remove a user from the current ticket")
@commands.has_permissions(manage_channels=True)
@app_commands.describe(user="User to remove from this ticket")
async def ticket_remove_cmd(ctx: commands.Context, user: discord.Member) -> None:
    ticket = data_manager.load_ticket_by_channel(ctx.channel.id)
    if not ticket:
        await ctx.send("This is not a ticket channel.", ephemeral=True)
        return
    # Don't allow removing the ticket creator
    if user.id == ticket.get('creator_id'):
        await ctx.send("You cannot remove the ticket creator.", ephemeral=True)
        return
    await ctx.channel.set_permissions(user, overwrite=None)
    await ctx.send(embed=discord.Embed(
        description=f"✅ {user.mention} has been removed from the ticket.",
        color=discord.Color.orange()
    ))
    logging.info(f"[Tickets] {ctx.author} removed {user} from ticket {ticket['ticket_id']}")


@bot.hybrid_command(name="rename", description="Rename the current ticket channel")
@commands.has_permissions(manage_channels=True)
@app_commands.describe(name="New channel name (no spaces)")
async def ticket_rename_cmd(ctx: commands.Context, *, name: str) -> None:
    ticket = data_manager.load_ticket_by_channel(ctx.channel.id)
    if not ticket:
        await ctx.send("This is not a ticket channel.", ephemeral=True)
        return
    clean_name = ''.join(c if c.isalnum() or c == '-' else '-' for c in name.lower())[:50]
    old_name = ctx.channel.name
    await ctx.channel.edit(name=clean_name)
    await ctx.send(embed=discord.Embed(
        description=f"✅ Channel renamed from `{old_name}` → `{clean_name}`",
        color=discord.Color.green()
    ))


@bot.hybrid_command(name="move", description="Move the ticket to a different panel category")
@commands.has_permissions(manage_channels=True)
@app_commands.describe(panel_id="Panel ID to move this ticket under")
async def ticket_move_cmd(ctx: commands.Context, panel_id: str) -> None:
    ticket = data_manager.load_ticket_by_channel(ctx.channel.id)
    if not ticket:
        await ctx.send("This is not a ticket channel.", ephemeral=True)
        return
    panel = data_manager.load_ticket_panel(panel_id)
    if not panel or panel['guild_id'] != ctx.guild.id:
        await ctx.send(f"Panel `{panel_id}` not found in this server.", ephemeral=True)
        return
    category_id = panel.get('category_id')
    if not category_id:
        await ctx.send("That panel has no category set.", ephemeral=True)
        return
    category = ctx.guild.get_channel(category_id)
    if not category:
        await ctx.send("Category channel not found.", ephemeral=True)
        return
    await ctx.channel.edit(category=category)
    ticket['panel_id'] = panel_id
    ticket['category'] = panel.get('name', 'General')
    data_manager.save_ticket(ticket)
    await ctx.send(embed=discord.Embed(
        description=f"✅ Ticket moved to **{panel.get('name', 'Unknown')}** (category: {category.name})",
        color=discord.Color.green()
    ))


@bot.hybrid_command(name="note", description="Add a private staff note to this ticket")
@commands.has_permissions(manage_channels=True)
@app_commands.describe(content="Note content (only staff can view these)")
async def ticket_note_cmd(ctx: commands.Context, *, content: str) -> None:
    ticket = data_manager.load_ticket_by_channel(ctx.channel.id)
    if not ticket:
        await ctx.send("This is not a ticket channel.", ephemeral=True)
        return
    import uuid
    note = {
        'note_id': str(uuid.uuid4())[:8],
        'ticket_id': ticket['ticket_id'],
        'guild_id': ctx.guild.id,
        'author_id': ctx.author.id,
        'content': content,
        'created_at': datetime.now(timezone.utc).isoformat(),
    }
    data_manager.save_ticket_note(note)
    await ctx.send(embed=discord.Embed(
        title="📝 Note Saved",
        description=content,
        color=discord.Color.yellow(),
        timestamp=datetime.now(timezone.utc)
    ).set_footer(text=f"By {ctx.author.display_name} • ID: {note['note_id']}"), ephemeral=True)


@bot.hybrid_command(name="notes", description="View all staff notes for this ticket")
@commands.has_permissions(manage_channels=True)
async def ticket_notes_cmd(ctx: commands.Context) -> None:
    ticket = data_manager.load_ticket_by_channel(ctx.channel.id)
    if not ticket:
        await ctx.send("This is not a ticket channel.", ephemeral=True)
        return
    notes = data_manager.load_ticket_notes(ticket['ticket_id'])
    if not notes:
        await ctx.send("No notes found for this ticket.", ephemeral=True)
        return
    embed = discord.Embed(
        title=f"📝 Notes for Ticket #{ticket['ticket_id']}",
        color=discord.Color.yellow(),
        timestamp=datetime.now(timezone.utc)
    )
    for note in notes[:10]:
        author = ctx.guild.get_member(note['author_id'])
        author_name = author.display_name if author else f"<@{note['author_id']}>"
        created = note['created_at'][:16].replace('T', ' ')
        embed.add_field(
            name=f"Note {note['note_id']} — {author_name} at {created}",
            value=note['content'][:1024],
            inline=False
        )
    await ctx.send(embed=embed, ephemeral=True)


@bot.hybrid_command(name="priority", description="Set the priority of the current ticket")
@commands.has_permissions(manage_channels=True)
@app_commands.describe(level="Priority level: low, normal, high, urgent")
@app_commands.choices(level=[
    app_commands.Choice(name="🟢 Low", value="low"),
    app_commands.Choice(name="🔵 Normal", value="normal"),
    app_commands.Choice(name="🟠 High", value="high"),
    app_commands.Choice(name="🔴 Urgent", value="urgent"),
])
async def ticket_priority_cmd(ctx: commands.Context, level: str) -> None:
    ticket = data_manager.load_ticket_by_channel(ctx.channel.id)
    if not ticket:
        await ctx.send("This is not a ticket channel.", ephemeral=True)
        return
    ticket['priority'] = level
    data_manager.save_ticket(ticket)
    emoji = PRIORITY_EMOJIS.get(level, '')
    color = PRIORITY_COLORS.get(level, discord.Color.blue())
    await ctx.send(embed=discord.Embed(
        description=f"{emoji} Ticket priority set to **{level.capitalize()}** by {ctx.author.mention}",
        color=color,
        timestamp=datetime.now(timezone.utc)
    ))


@bot.hybrid_command(name="reopen", description="Reopen a closed ticket")
@commands.has_permissions(manage_channels=True)
@app_commands.describe(ticket_id="The ticket ID to reopen")
async def ticket_reopen_cmd(ctx: commands.Context, ticket_id: str) -> None:
    """Reopen a closed ticket by recreating its channel."""
    if not ticket_tool:
        await ctx.send("Ticket system not initialized.")
        return
    ticket = data_manager.load_ticket(ticket_id)
    if not ticket:
        await ctx.send(f"Ticket `{ticket_id}` not found.")
        return
    if ticket['guild_id'] != ctx.guild.id:
        await ctx.send("That ticket does not belong to this server.")
        return
    if ticket.get('status') == 'open':
        existing_channel = ctx.guild.get_channel(ticket['channel_id'])
        if existing_channel:
            await ctx.send(f"That ticket is already open: {existing_channel.mention}")
            return

    # Rebuild the channel
    creator = ctx.guild.get_member(ticket['creator_id'])
    if not creator:
        await ctx.send("Cannot reopen — the original ticket creator is no longer in the server.")
        return

    # Use the panel if available, otherwise use default settings
    panel = data_manager.load_ticket_panel(ticket.get('panel_id', '')) or {}
    settings = data_manager.load_ticket_settings(ctx.guild.id) or {}
    category_id = panel.get('category_id') or settings.get('category_id')
    support_role_id = panel.get('support_role_id') or settings.get('support_role_id')

    overwrites = {
        ctx.guild.default_role: discord.PermissionOverwrite(view_channel=False),
        creator: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, attach_files=True),
        ctx.guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True, read_message_history=True),
    }
    if support_role_id:
        role = ctx.guild.get_role(support_role_id)
        if role:
            overwrites[role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, attach_files=True)

    category = ctx.guild.get_channel(category_id) if category_id else None
    channel_name = f"ticket-{creator.display_name}".lower()[:50]
    channel_name = ''.join(c if c.isalnum() or c == '-' else '-' for c in channel_name)

    try:
        new_channel = await ctx.guild.create_text_channel(
            channel_name,
            category=category,
            overwrites=overwrites,
            topic=f"Ticket {ticket_id} (reopened) - {creator}"
        )
    except Exception as e:
        await ctx.send(f"Failed to create channel: {e}")
        return

    ticket['channel_id'] = new_channel.id
    ticket['status'] = 'open'
    ticket['closed_at'] = None
    ticket['closed_by'] = None
    ticket['close_reason'] = None
    data_manager.save_ticket(ticket)

    control_view = TicketControlView(ticket_id)
    await new_channel.send(
        embed=discord.Embed(
            title=f"🔓 Ticket Reopened — #{ticket_id}",
            description=f"This ticket was reopened by {ctx.author.mention}.\n{creator.mention} your ticket has been reopened.",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc)
        ),
        view=control_view
    )
    await ctx.send(f"Ticket `{ticket_id}` reopened: {new_channel.mention}")
    logging.info(f"[Tickets] {ctx.author} reopened ticket {ticket_id}")


@bot.hybrid_command(name="ticketstats", description="View ticket statistics for this server")
@commands.has_permissions(manage_channels=True)
async def ticket_stats_cmd(ctx: commands.Context) -> None:
    stats = data_manager.load_ticket_stats(ctx.guild.id)
    embed = discord.Embed(
        title=f"📊 Ticket Statistics — {ctx.guild.name}",
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(name="Total Tickets", value=str(stats['total']), inline=True)
    embed.add_field(name="Open", value=str(stats['open']), inline=True)
    embed.add_field(name="Closed", value=str(stats['closed']), inline=True)

    avg_rating = f"⭐ {stats['avg_rating']}" if stats['avg_rating'] else "No ratings yet"
    embed.add_field(name="Avg Rating", value=avg_rating, inline=True)

    avg_close = f"{stats['avg_close_hours']}h" if stats['avg_close_hours'] is not None else "N/A"
    embed.add_field(name="Avg Close Time", value=avg_close, inline=True)

    priorities = stats.get('priorities', {})
    if priorities:
        prio_text = "\n".join(
            f"{PRIORITY_EMOJIS.get(k, '')} {k.capitalize()}: {v}"
            for k, v in priorities.items()
        )
    else:
        prio_text = "No open tickets"
    embed.add_field(name="Open by Priority", value=prio_text, inline=False)

    await ctx.send(embed=embed)


@bot.hybrid_command(name="dbcleanup", description="Scan the database and remove stale, invalid, or unused data")
@commands.has_permissions(administrator=True)
async def dbcleanup_cmd(ctx: commands.Context) -> None:
    """
    Scans every ticket-related table and cross-checks against live Discord state.
    Removes / closes anything that no longer has a valid corresponding Discord object.

    What it checks:
      - Panels      : is the Discord channel and message still there?
      - Open tickets : does the ticket channel still exist in Discord?
      - Closed tickets: are their answers, notes, messages, transcripts still linked to a real ticket?
      - Blacklist    : are entries expired or already inactive?
      - Warnings     : are there deactivated warning rows taking up space?
    """
    await ctx.defer()

    status_msg = await ctx.send(
        embed=discord.Embed(
            title="🔍 Database Cleanup — Scanning...",
            description="Checking every table against live Discord state. Please wait.",
            color=discord.Color.yellow()
        )
    )

    report_lines: List[str] = []

    # ─── 1. PANELS ──────────────────────────────────────────────────────────────
    all_panels = data_manager.load_all_ticket_panels()
    valid_panel_ids: set = set()
    panels_deactivated = 0

    for panel in all_panels:
        if not panel.get('is_active'):
            continue  # Already inactive, skip
        guild = bot.get_guild(panel['guild_id'])
        if not guild:
            # Bot no longer in that guild — deactivate
            panel['is_active'] = 0
            data_manager.save_ticket_panel(panel)
            panels_deactivated += 1
            continue
        channel = guild.get_channel(panel.get('channel_id', 0))
        if not channel:
            panel['is_active'] = 0
            data_manager.save_ticket_panel(panel)
            panels_deactivated += 1
            continue
        # Try to verify the panel message still exists
        msg_id = panel.get('message_id')
        if msg_id:
            try:
                await channel.fetch_message(msg_id)
            except (discord.NotFound, discord.Forbidden):
                panel['is_active'] = 0
                data_manager.save_ticket_panel(panel)
                panels_deactivated += 1
                continue
        valid_panel_ids.add(panel['panel_id'])

    if panels_deactivated:
        report_lines.append(f"🗂️ **Panels** — deactivated **{panels_deactivated}** (channel/message gone)")
    else:
        report_lines.append("🗂️ **Panels** — ✅ all active panels valid")

    # ─── 2. TICKETS ──────────────────────────────────────────────────────────
    all_tickets = data_manager.load_all_tickets()
    valid_ticket_ids: set = set()
    closed_ticket_ids: set = set()
    orphaned_open_ticket_ids: set = set()

    for ticket in all_tickets:
        if ticket.get('status') == 'closed':
            # All closed tickets get purged — they're done, no longer needed
            closed_ticket_ids.add(ticket['ticket_id'])
        else:
            # Open ticket — check if its Discord channel still exists
            valid_ticket_ids.add(ticket['ticket_id'])
            guild = bot.get_guild(ticket['guild_id'])
            if not guild:
                orphaned_open_ticket_ids.add(ticket['ticket_id'])
                continue
            channel = guild.get_channel(ticket.get('channel_id', 0))
            if not channel:
                orphaned_open_ticket_ids.add(ticket['ticket_id'])

    if closed_ticket_ids:
        report_lines.append(
            f"🎫 **Closed Tickets** — permanently deleting **{len(closed_ticket_ids)}** "
            f"closed ticket(s) and all their linked data (answers, notes, messages, transcripts)"
        )
    else:
        report_lines.append("🎫 **Closed Tickets** — ✅ none to remove")

    if orphaned_open_ticket_ids:
        report_lines.append(
            f"⚠️ **Orphaned Open Tickets** — marking **{len(orphaned_open_ticket_ids)}** "
            f"as closed (Discord channel no longer exists)"
        )

    # ─── 3. BLACKLIST ────────────────────────────────────────────────────────
    all_blacklist = data_manager.load_all_ticket_blacklist()
    expired_blacklist_ids: set = set()
    now_utc = datetime.now(timezone.utc)

    for entry in all_blacklist:
        if not entry.get('is_active'):
            expired_blacklist_ids.add(entry['blacklist_id'])
            continue
        expires_at = entry.get('expires_at')
        if expires_at:
            try:
                exp_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                if now_utc > exp_dt:
                    expired_blacklist_ids.add(entry['blacklist_id'])
            except Exception:
                pass

    if expired_blacklist_ids:
        report_lines.append(
            f"🚫 **Ticket Blacklist** — removing **{len(expired_blacklist_ids)}** "
            f"expired/inactive entries"
        )
    else:
        report_lines.append("🚫 **Ticket Blacklist** — ✅ no expired entries")

    # ─── 4. COUNT ORPHANED CHILD ROWS (preview before deleting) ─────────────
    # These are counted here for reporting; the actual DELETE happens in purge_stale_data
    cursor = data_manager._connection.cursor()

    def count_orphans(table: str, fk_col: str, valid_ids: set) -> int:
        if not valid_ids:
            cursor.execute(f'SELECT COUNT(*) FROM {table}')
        else:
            ph = ','.join('?' * len(valid_ids))
            cursor.execute(
                f'SELECT COUNT(*) FROM {table} WHERE {fk_col} NOT IN ({ph})',
                list(valid_ids)
            )
        return cursor.fetchone()[0]

    orphan_answers    = count_orphans('ticket_answers',     'ticket_id', valid_ticket_ids)
    orphan_notes      = count_orphans('ticket_notes',       'ticket_id', valid_ticket_ids)
    orphan_messages   = count_orphans('ticket_messages',    'ticket_id', valid_ticket_ids)
    orphan_transcripts= count_orphans('ticket_transcripts', 'ticket_id', valid_ticket_ids)
    orphan_questions  = count_orphans('ticket_questions',   'panel_id',  valid_panel_ids)

    cursor.execute('SELECT COUNT(*) FROM warnings WHERE is_active = 0')
    inactive_warnings = cursor.fetchone()[0]

    child_total = orphan_answers + orphan_notes + orphan_messages + orphan_transcripts + orphan_questions

    if child_total:
        report_lines.append(
            f"🗑️ **Orphaned rows** found:\n"
            f"  • Answers: {orphan_answers}\n"
            f"  • Notes: {orphan_notes}\n"
            f"  • Cached messages: {orphan_messages}\n"
            f"  • Transcripts: {orphan_transcripts}\n"
            f"  • Questions: {orphan_questions}"
        )
    else:
        report_lines.append("🗑️ **Orphaned child rows** — ✅ none found")

    if inactive_warnings:
        report_lines.append(f"⚠️ **Warnings** — removing **{inactive_warnings}** deactivated rows")
    else:
        report_lines.append("⚠️ **Warnings** — ✅ no inactive rows")

    # ─── 5. NOTHING TO DO? ──────────────────────────────────────────────────
    nothing_to_do = (
        panels_deactivated == 0
        and len(closed_ticket_ids) == 0
        and len(orphaned_open_ticket_ids) == 0
        and len(expired_blacklist_ids) == 0
        and child_total == 0
        and inactive_warnings == 0
    )

    if nothing_to_do:
        await status_msg.edit(embed=discord.Embed(
            title="✅ Database Cleanup — Nothing to clean",
            description="Every table was checked. All rows are valid and in use.",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc)
        ))
        return

    # ─── 6. CONFIRM VIEW ────────────────────────────────────────────────────
    preview_embed = discord.Embed(
        title="🔍 Database Cleanup — Review",
        description="\n".join(report_lines),
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc)
    )
    preview_embed.set_footer(text="Press Confirm to permanently apply these changes, or Cancel to abort.")

    confirm_view = DBCleanupConfirmView(
        ctx.author.id,
        valid_ticket_ids=valid_ticket_ids,
        valid_panel_ids=valid_panel_ids,
        orphaned_open_ticket_ids=orphaned_open_ticket_ids,
        expired_blacklist_ids=expired_blacklist_ids,
        closed_ticket_ids=closed_ticket_ids,
        report_lines=report_lines,
    )
    await status_msg.edit(embed=preview_embed, view=confirm_view)


class DBCleanupConfirmView(View):
    def __init__(
        self,
        user_id: int,
        valid_ticket_ids: set,
        valid_panel_ids: set,
        orphaned_open_ticket_ids: set,
        expired_blacklist_ids: set,
        closed_ticket_ids: set,
        report_lines: List[str],
    ):
        super().__init__(timeout=120)
        self.user_id = user_id
        self.valid_ticket_ids = valid_ticket_ids
        self.valid_panel_ids = valid_panel_ids
        self.orphaned_open_ticket_ids = orphaned_open_ticket_ids
        self.expired_blacklist_ids = expired_blacklist_ids
        self.closed_ticket_ids = closed_ticket_ids
        self.report_lines = report_lines

    @discord.ui.button(label="✅ Confirm Cleanup", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Only the person who ran this command can confirm.", ephemeral=True)
            return

        await interaction.response.defer()

        counts = data_manager.purge_stale_data(
            valid_ticket_ids=self.valid_ticket_ids,
            valid_panel_ids=self.valid_panel_ids,
            orphaned_open_ticket_ids=self.orphaned_open_ticket_ids,
            expired_blacklist_ids=self.expired_blacklist_ids,
            closed_ticket_ids=self.closed_ticket_ids,
        )

        total_removed = sum(counts.values())

        result_lines = self.report_lines + [
            "",
            "**Rows removed:**",
            f"  • Closed tickets deleted: {counts.get('closed_tickets', 0)}",
            f"  • Answers deleted: {counts.get('closed_answers', 0) + counts.get('orphan_answers', 0)}",
            f"  • Notes deleted: {counts.get('closed_notes', 0) + counts.get('orphan_notes', 0)}",
            f"  • Cached messages deleted: {counts.get('closed_messages', 0) + counts.get('orphan_messages', 0)}",
            f"  • Transcripts deleted: {counts.get('closed_transcripts', 0) + counts.get('orphan_transcripts', 0)}",
            f"  • Questions deleted: {counts.get('orphan_questions', 0)}",
            f"  • Open tickets closed (orphaned): {counts.get('orphaned_tickets_closed', 0)}",
            f"  • Blacklist entries removed: {counts.get('ticket_blacklist', 0)}",
            f"  • Inactive warnings removed: {counts.get('warnings', 0)}",
            "",
            f"**Total rows cleaned: {total_removed}**",
        ]

        for child in self.children:
            child.disabled = True

        await interaction.edit_original_response(
            embed=discord.Embed(
                title="✅ Database Cleanup — Complete",
                description="\n".join(result_lines),
                color=discord.Color.green(),
                timestamp=datetime.now(timezone.utc)
            ),
            view=self
        )
        logging.info(f"[DBCleanup] Cleanup run by {interaction.user}: {counts}")
        self.stop()

    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: Button) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Only the person who ran this command can cancel.", ephemeral=True)
            return
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(
            embed=discord.Embed(
                title="❌ Database Cleanup — Cancelled",
                description="No changes were made.",
                color=discord.Color.red(),
                timestamp=datetime.now(timezone.utc)
            ),
            view=self
        )
        self.stop()


# GIVEAWAY COMMANDS (V2 Enhancement)
# =============================================================================
@bot.hybrid_command(name="giveaway", description="Create a giveaway")
@commands.has_permissions(manage_guild=True)
@app_commands.describe(duration="Duration in hours", winners="Number of winners", prize="The prize")
async def giveaway_cmd(ctx: commands.Context, duration: int, winners: int, *, prize: str) -> None:
    if not config.enable_giveaways:
        await ctx.send("Giveaway system is disabled.")
        return
    
    if winners > config.limits.max_giveaway_winners:
        await ctx.send(f"Maximum {config.limits.max_giveaway_winners} winners allowed.")
        return
    
    import uuid
    giveaway_id = str(uuid.uuid4())[:8]
    
    giveaway = {
        'giveaway_id': giveaway_id,
        'message_id': 0,
        'channel_id': ctx.channel.id,
        'guild_id': ctx.guild.id,
        'host_id': ctx.author.id,
        'prize': prize,
        'winner_count': winners,
        'entries': [],
        'status': 'active',
        'created_at': datetime.now(timezone.utc).isoformat(),
        'ends_at': (datetime.now(timezone.utc) + timedelta(hours=duration)).isoformat()
    }
    
    embed = EmbedBuilder.giveaway(
        prize,
        f"**Hosted by:** {ctx.author.mention}\n"
        f"**Winners:** {winners}\n"
        f"**Ends:** <t:{int(datetime.fromisoformat(giveaway['ends_at']).timestamp())}:R>\n"
        f"**Entries:** 0\n\n"
        f"Click the button below to enter!"
    )
    
    view = GiveawayView(giveaway_id)
    message = await ctx.send(embed=embed, view=view)
    
    giveaway['message_id'] = message.id
    giveaways_data[giveaway_id] = giveaway
    save_giveaways_data()
    
    logging.info(f"[Giveaway] Created by {ctx.author}: {prize}")


@bot.hybrid_command(name="endgiveaway", description="End a giveaway early")
@commands.has_permissions(manage_guild=True)
@app_commands.describe(giveaway_id="The giveaway ID to end")
async def endgiveaway_cmd(ctx: commands.Context, giveaway_id: str) -> None:
    if giveaway_id not in giveaways_data:
        await ctx.send("Giveaway not found.")
        return
    
    await end_giveaway(giveaway_id)
    await ctx.send(f"Giveaway `{giveaway_id}` has been ended.")


# --- LEVEL COMMANDS (V2 Enhancement) ---
@bot.hybrid_command(name="level", description="View your level")
@app_commands.describe(member="Member to check")
async def level_cmd(ctx: commands.Context, member: Optional[discord.Member] = None) -> None:
    if not config.enable_leveling:
        await ctx.send("Leveling system is disabled.")
        return
    
    member = member or ctx.author
    key = (member.id, ctx.guild.id)
    level_data = levels_data.get(key, {'xp': 0, 'level': 0, 'total_messages': 0})
    
    embed = EmbedBuilder.level(member, level_data)
    await ctx.send(embed=embed)


@bot.hybrid_command(name="leaderboard", description="View the server leaderboard")
async def leaderboard_cmd(ctx: commands.Context) -> None:
    if not config.enable_leveling:
        await ctx.send("Leveling system is disabled.")
        return
    
    # Get top 10 users in this guild
    guild_levels = [(uid, data) for (uid, gid), data in levels_data.items() if gid == ctx.guild.id]
    guild_levels.sort(key=lambda x: x[1].get('xp', 0), reverse=True)
    
    if not guild_levels:
        await ctx.send("No data yet. Start chatting to earn XP!")
        return
    
    embed = EmbedBuilder.info("ðŸ† Leaderboard", "")
    
    medals = ["ðŸ¥‡", "ðŸ¥ˆ", "ðŸ¥‰"]
    
    for idx, (user_id, data) in enumerate(guild_levels[:10]):
        medal = medals[idx] if idx < 3 else f"#{idx + 1}"
        user = ctx.guild.get_member(user_id)
        name = user.display_name if user else f"User {user_id}"
        
        embed.add_field(
            name=f"{medal} {name}",
            value=f"Level {data.get('level', 0)} â€¢ {data.get('xp', 0):,} XP",
            inline=False
        )
    
    await ctx.send(embed=embed)


# --- INFORMATION COMMANDS ---
@bot.command()
async def userinfo(ctx: commands.Context, member: discord.Member) -> None:
    embed = discord.Embed(title=f"{member.name}'s Info", color=discord.Color.blue())
    embed.add_field(name="ID", value=member.id)
    embed.add_field(name="Joined", value=member.joined_at)
    embed.add_field(name="Top Role", value=member.top_role)
    await ctx.send(embed=embed)


@bot.command()
async def serverinfo(ctx: commands.Context) -> None:
    try:
        guild = ctx.guild
        online = sum(1 for m in guild.members if m.status != discord.Status.offline)
        bots = sum(1 for m in guild.members if m.bot)
        humans = guild.member_count - bots
        
        embed = discord.Embed(title=f"{guild.name} Server Info", color=discord.Color.blurple())
        embed.add_field(name="Server ID", value=guild.id, inline=True)
        embed.add_field(name="Owner", value=guild.owner.mention, inline=True)
        embed.add_field(name="Created", value=guild.created_at.strftime("%B %d, %Y"), inline=True)
        embed.add_field(name=f"Members ({guild.member_count})", value=f"Online: {online}\nOffline: {guild.member_count - online}\nBots: {bots}\nHumans: {humans}", inline=False)
        embed.add_field(name=f"Channels ({len(guild.text_channels) + len(guild.voice_channels)})", value=f"Text: {len(guild.text_channels)}\nVoice: {len(guild.voice_channels)}", inline=True)
        embed.add_field(name="Other Stats", value=f"Roles: {len(guild.roles)}\nBoosts: {guild.premium_subscription_count}", inline=True)
        
        if guild.icon:
            embed.set_thumbnail(url=guild.icon.url)
        
        await ctx.send(embed=embed)
    except Exception as e:
        await ctx.send("An error occurred")
        logging.error(f"Error in serverinfo: {str(e)}")


@bot.command()
async def membercount(ctx: commands.Context) -> None:
    member_role = ctx.guild.get_role(config.roles.member)
    if member_role is None:
        await ctx.send("Member role not found.")
        return
    members_with_role = [m for m in ctx.guild.members if member_role in m.roles]
    await ctx.send(f'There are {len(members_with_role)} members with the member role.')


@bot.command()
async def ping(ctx: commands.Context) -> None:
    await ctx.send(f'Pong! {round(bot.latency * 1000)}ms')


@bot.command()
async def uptime_cmd(ctx: commands.Context) -> None:
    await ctx.send(f"Bot uptime: {get_uptime()}")


# --- INVITE MANAGEMENT COMMANDS ---
@bot.hybrid_command(name="setupinvites", description="Set up the invite tracking system")
@commands.has_permissions(manage_guild=True)
async def setupinvites_cmd(ctx: commands.Context) -> None:
    global invite_manager
    
    if invite_manager.invite_message_id:
        try:
            old_channel = bot.get_channel(invite_manager.invite_channel_id)
            if old_channel:
                old_message = await old_channel.fetch_message(invite_manager.invite_message_id)
                await old_message.delete()
        except Exception:
            pass
    
    message = await invite_manager.post_initial_message(ctx.channel)
    new_invites = await invite_manager.generate_all_invites(ctx.guild)
    
    if not new_invites:
        await ctx.send("Failed to create invite links. Make sure I have permission to create invites.")
        return
    
    invite_manager.tracked_invites = new_invites
    invite_manager.save_data()
    await invite_manager.update_invite_message(ctx.channel)
    
    await ctx.send(embed=EmbedBuilder.success("Invite System Setup", f"Created **{len(new_invites)}** invite links."))
    logging.info(f"[InviteManager] Setup completed by {ctx.author} in channel {ctx.channel.name}")


@bot.hybrid_command(name="regenerateinvites", description="Regenerate all invite links")
@commands.has_permissions(manage_guild=True)
async def regenerateinvites_cmd(ctx: commands.Context) -> None:
    global invite_manager
    
    if not invite_manager.invite_channel_id:
        await ctx.send("Invite system not set up. Use `/setupinvites` first.")
        return
    
    channel = bot.get_channel(invite_manager.invite_channel_id)
    if not channel:
        await ctx.send("Invite channel not found.")
        return
    
    await ctx.send("Deleting old invite links...")
    
    try:
        guild_invites = await ctx.guild.invites()
        deleted_count = 0
        
        for invite in guild_invites:
            if invite.code in invite_manager.tracked_invites:
                try:
                    await invite.delete(reason="Regenerating invite links")
                    deleted_count += 1
                except Exception as e:
                    logging.warning(f"[InviteManager] Could not delete invite {invite.code}: {e}")
    except discord.Forbidden:
        await ctx.send("I don't have permission to manage invites.")
        return
    except Exception as e:
        await ctx.send(f"Error deleting old invites: {e}")
        return
    
    new_invites = await invite_manager.generate_all_invites(ctx.guild)
    
    if not new_invites:
        await ctx.send("Failed to create new invites.")
        return
    
    invite_manager.tracked_invites = new_invites
    invite_manager.save_data()
    await invite_manager.update_invite_message(channel)
    
    await ctx.send(embed=EmbedBuilder.success("Invites Regenerated", f"Deleted **{deleted_count}** old invites and generated **{len(new_invites)}** new ones!"))
    logging.info(f"[InviteManager] Invites regenerated by {ctx.author}")


@bot.hybrid_command(name="checkinvites", description="Display the status of all tracked invites")
async def checkinvites_cmd(ctx: commands.Context) -> None:
    global invite_manager
    
    if not invite_manager.tracked_invites:
        await ctx.send("No invites are being tracked. Use `/setupinvites` to set up.")
        return
    
    try:
        live_invites = await ctx.guild.invites()
        live_invite_data = {inv.code: inv for inv in live_invites}
    except Exception:
        live_invite_data = {}
    
    embed = discord.Embed(title="Invite Status Report", color=discord.Color.blue())
    active_count = 0
    expired_count = 0
    
    for code, data in invite_manager.tracked_invites.items():
        max_uses = data.get('max_uses', 0)
        current_uses = live_invite_data[code].uses if code in live_invite_data else data.get('uses', 0)
        remaining = max_uses - current_uses
        is_expired = data['status'] == 'expired' or data.get('expired') or (remaining <= 0 and max_uses > 0)
        
        if is_expired:
            status_emoji = "âŒ"
            status_text = "EXPIRED"
            expired_count += 1
        else:
            status_emoji = "âœ…"
            status_text = f"{remaining}/{max_uses} Remaining"
            active_count += 1
        
        embed.add_field(name=f"{status_emoji} `discord.gg/{code}`", value=f"**{data['name']}**\n{current_uses}/{max_uses} Used | {status_text}", inline=True)
    
    embed.add_field(name="Summary", value=f"Active: {active_count}\nExpired: {expired_count}", inline=False)
    
    await ctx.send(embed=embed)


@bot.hybrid_command(name="inviteinfo", description="Display detailed information about a specific invite")
@app_commands.describe(invite_code="The invite code to check")
@app_commands.autocomplete(invite_code=invite_code_autocomplete)
async def inviteinfo_cmd(ctx: commands.Context, invite_code: Optional[str] = None) -> None:
    global invite_manager
    
    if not invite_manager.tracked_invites:
        await ctx.send("No invites are being tracked.")
        return
    
    if not invite_code:
        await checkinvites_cmd(ctx)
        return
    
    invite_code = invite_code.replace("discord.gg/", "").replace("gg/", "").strip()
    
    if invite_code not in invite_manager.tracked_invites:
        await ctx.send(f"Invite `{invite_code}` is not being tracked.")
        return
    
    data = invite_manager.tracked_invites[invite_code]
    max_uses = data.get('max_uses', 0)
    
    try:
        live_invites = await ctx.guild.invites()
        current_uses = next((inv.uses for inv in live_invites if inv.code == invite_code), data.get('uses', 0))
    except Exception:
        current_uses = data.get('uses', 0)
    
    remaining = max_uses - current_uses
    is_expired = data['status'] == 'expired' or data.get('expired') or (remaining <= 0 and max_uses > 0)
    
    embed = discord.Embed(title=f"Invite: discord.gg/{invite_code}", color=discord.Color.green() if not is_expired else discord.Color.red())
    embed.add_field(name="Name", value=data['name'], inline=True)
    embed.add_field(name="Status", value="EXPIRED" if is_expired else "ACTIVE", inline=True)
    embed.add_field(name="Uses", value=f"{current_uses}/{max_uses} Used", inline=True)
    embed.add_field(name="Remaining", value=f"**{remaining}** uses left" if not is_expired else "None", inline=True)
    
    created_at = data.get('created_at', 'Unknown')
    embed.add_field(name="Created At", value=created_at[:10] if created_at != 'Unknown' else 'Unknown', inline=True)
    
    await ctx.send(embed=embed)


# --- VERIFICATION COMMANDS ---
@bot.command()
@commands.has_permissions(manage_roles=True)
async def verifyuser(ctx: commands.Context, member: discord.Member) -> None:
    role = ctx.guild.get_role(config.roles.verified)
    if role not in member.roles:
        await member.add_roles(role)
        await ctx.send(embed=EmbedBuilder.success("User Verified", f"Welcome {member.mention} to MOS! You have been verified."))
        logging.info(f'User {member} was verified by {ctx.author}')
    else:
        await ctx.send(f"{member.mention} is already verified.")


@bot.command()
@commands.has_permissions(manage_roles=True)
async def securitycheck(ctx: commands.Context, member: discord.Member) -> None:
    embed = discord.Embed(title=f"Security Check: {member.display_name}", color=discord.Color.blue())
    
    now = datetime.now(timezone.utc)
    created_at = member.created_at.replace(tzinfo=timezone.utc) if member.created_at.tzinfo is None else member.created_at
    account_age = (now - created_at).days
    
    age_risk = "HIGH" if account_age < 13 else "MEDIUM" if account_age < 30 else "LOW"
    embed.add_field(name="Account Age", value=f"{account_age} days ({age_risk} risk)", inline=True)
    
    join_age = (now - member.joined_at.replace(tzinfo=timezone.utc)).days if member.joined_at else "Unknown"
    embed.add_field(name="Time in Server", value=f"{join_age} days", inline=True)
    
    profile_flags: List[str] = []
    if not member.avatar:
        profile_flags.append("No profile picture")
    if len(member.display_name) < 3:
        profile_flags.append("Very short username")
    if member.display_name.isdigit():
        profile_flags.append("Username is all numbers")
    
    profile_risk = "HIGH" if len(profile_flags) >= 2 else "MEDIUM" if profile_flags else "LOW"
    embed.add_field(name="Profile Risk", value=f"{profile_risk}\n{', '.join(profile_flags) if profile_flags else 'No flags'}", inline=False)
    
    embed.set_thumbnail(url=member.avatar.url if member.avatar else member.default_avatar.url)
    embed.set_footer(text=f"User ID: {member.id}")
    
    await ctx.send(embed=embed)


@bot.command()
@commands.cooldown(1, 3600, commands.BucketType.user)
async def verify(ctx: commands.Context) -> None:
    now = datetime.now(timezone.utc)
    created_at = ctx.author.created_at.replace(tzinfo=timezone.utc) if ctx.author.created_at.tzinfo is None else ctx.author.created_at
    
    if (now - created_at).days < config.limits.min_account_age_days:
        await ctx.send("Your account is too new to verify.")
        return
    
    member_role = ctx.guild.get_role(config.roles.member)
    if member_role and member_role in ctx.author.roles:
        await ctx.send("You are already verified!")
        return
    
    process_manager.add_verification(ctx.author.id)

    mainchannel = bot.get_channel(config.channels.verification_main)

    try:
        await ctx.author.send(embed=EmbedBuilder.verification("MOS Verification - Application", "Welcome! Please answer the following questions carefully to apply."))
    except discord.Forbidden:
        process_manager.remove_verification(ctx.author.id)
        await mainchannel.send(
            f"❌ {ctx.author.mention}, I couldn't send you a DM! "
            f"Please enable **Direct Messages** from server members in your privacy settings and try again."
        )
        return
    except Exception as e:
        process_manager.remove_verification(ctx.author.id)
        logging.error(f"Verification DM failed for {ctx.author}: {str(e)}")
        await mainchannel.send(f"❌ {ctx.author.mention}, something went wrong while starting your verification. Please try again or contact staff.")
        return

    await mainchannel.send(f"Check your DM's {ctx.author.mention}..")

    try:
        responses = await _collect_verification_responses(ctx)
        if responses is None:
            return

        confirmed = await _confirm_submission(ctx)
        if not confirmed:
            return

        await _submit_verification(ctx, responses)

    except asyncio.TimeoutError:
        process_manager.remove_verification(ctx.author.id)
        try:
            await ctx.author.send("Verification timed out. Please run `!verify` again when you're ready.")
        except discord.Forbidden:
            pass
    except Exception as e:
        process_manager.remove_verification(ctx.author.id)
        logging.error(f"Verification error for {ctx.author}: {str(e)}")
        try:
            await ctx.author.send("An error occurred during verification. Please contact staff.")
        except discord.Forbidden:
            await mainchannel.send(f"⚠️ {ctx.author.mention}, an error occurred during your verification. Please contact staff.")


async def _collect_verification_responses(ctx: commands.Context) -> Optional[Dict[str, Optional[str]]]:
    responses: Dict[str, Optional[str]] = {}
    roblox_username = None  # Store for verification
    
    for idx, (question, label) in enumerate(VERIFICATION_QUESTIONS, 1):
        # Handle Roblox Verification step
        if label == "Roblox Verification" and question == "ROBLOX_VERIFICATION_STEP":
            if roblox_username is None:
                # Skip if no roblox username was captured
                continue
            
            # Send verification instructions
            verification_embed = discord.Embed(
                title="🎮 ROBLOX Account Verification",
                description=(
                    f"Let's verify your ROBLOX account: **{roblox_username}**\n\n"
                    f"**Steps:**\n"
                    f"1️⃣ Join our verification game: [Click Here to Join]({config.roblox.verification_game_url})\n"
                    f"2️⃣ Once in the game, click the button to **Generate Code**\n"
                    f"3️⃣ Copy the **6-character code** shown\n"
                    f"4️⃣ Paste the code here\n\n"
                    f"⚠️ **Important:** The code is unique to your account. Do not share it with anyone!"
                ),
                color=discord.Color.blue()
            )
            verification_embed.set_footer(text="⏱️ You have 5 minutes to complete verification")
            
            await ctx.author.send(embed=verification_embed)
            
            # Wait for code input with retries
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    response = await bot.wait_for(
                        'message', 
                        check=lambda m: m.author == ctx.author and m.channel.type == discord.ChannelType.private, 
                        timeout=300  # 5 minutes
                    )
                    
                    code = response.content.strip().upper()
                    
                    # Verify the code
                    if code in verification_codes:
                        stored_data = verification_codes[code]
                        stored_username = stored_data.get('roblox_username', '').lower()
                        
                        if stored_username == roblox_username.lower():
                            # Success! Code matches the username
                            responses[label] = f"✅ Verified: {roblox_username}"
                            
                            # Remove the used code
                            del verification_codes[code]
                            
                            success_embed = discord.Embed(
                                title="✅ Verification Successful!",
                                description=f"Your ROBLOX account **{roblox_username}** has been verified!",
                                color=discord.Color.green()
                            )
                            await ctx.author.send(embed=success_embed)
                            break
                        else:
                            # Code belongs to different user
                            await ctx.author.send(embed=EmbedBuilder.warning(
                                "❌ Code Mismatch",
                                f"This code was generated for a different ROBLOX account. "
                                f"Please generate a new code while logged in as **{roblox_username}**."
                            ))
                    else:
                        # Invalid or expired code
                        remaining = max_attempts - attempt - 1
                        if remaining > 0:
                            await ctx.author.send(embed=EmbedBuilder.warning(
                                "❌ Invalid Code",
                                f"That code is invalid or expired. Please make sure you:\n"
                                f"• Joined the correct game\n"
                                f"• Generated a new code\n"
                                f"• Copied the code exactly\n\n"
                                f"**{remaining}** attempt(s) remaining."
                            ))
                        else:
                            process_manager.remove_verification(ctx.author.id)
                            await ctx.author.send(embed=EmbedBuilder.error(
                                "❌ Verification Failed",
                                "Too many invalid attempts. Please run `!verify` again."
                            ))
                            return None
                            
                except asyncio.TimeoutError:
                    process_manager.remove_verification(ctx.author.id)
                    await ctx.author.send("⚠️ Roblox verification timed out. Please run `!verify` again.")
                    return None
            
            continue  # Move to next question
        
        # Handle Agreement step (existing code)
        if label == "Agreement":
            agreement_embed = EmbedBuilder.verification("Agreement - Terms & Rules", question)
            agreement_embed.add_field(name="Instructions", value="Click the buttons below to view the rules for both servers.\nAfter reviewing, type **'I agree'** to continue.", inline=False)
            
            rules_view = RulesButtonView(ctx.author.id, timeout=300)
            await ctx.author.send(embed=agreement_embed, view=rules_view)
            
            try:
                response = await bot.wait_for('message', check=lambda m: m.author == ctx.author and m.channel.type == discord.ChannelType.private, timeout=config.timing.verification_timeout_seconds)
                
                if response.content.lower().strip() in ['i agree', 'i agree.', 'agree', 'yes', 'y', 'i accept']:
                    responses[label] = "Agreed to Terms & Rules"
                else:
                    await ctx.author.send(embed=EmbedBuilder.warning("Invalid Response", "Please type **'I agree'** to accept the terms and continue, or type **'cancel'** to exit."))
                    
                    retry_response = await bot.wait_for('message', check=lambda m: m.author == ctx.author and m.channel.type == discord.ChannelType.private, timeout=60)
                    
                    if retry_response.content.lower().strip() in ['cancel', 'exit', 'quit', 'no', 'n']:
                        process_manager.remove_verification(ctx.author.id)
                        await ctx.author.send("Verification cancelled.")
                        return None
                    elif retry_response.content.lower().strip() in ['i agree', 'i agree.', 'agree', 'yes', 'y', 'i accept']:
                        responses[label] = "Agreed to Terms & Rules"
                    else:
                        process_manager.remove_verification(ctx.author.id)
                        await ctx.author.send("Verification cancelled. Please try again when ready to agree to the rules.")
                        return None
            except asyncio.TimeoutError:
                process_manager.remove_verification(ctx.author.id)
                await ctx.author.send("Verification timed out. Please try again.")
                return None
        
        # Capture Roblox Username for later verification
        elif label == "ROBLOX Username":
            await ctx.author.send(embed=EmbedBuilder.info(f"Question {idx} of {len(VERIFICATION_QUESTIONS)}", question))
            
            try:
                response = await bot.wait_for('message', check=lambda m: m.author == ctx.author and m.channel.type == discord.ChannelType.private, timeout=config.timing.verification_timeout_seconds)
                roblox_username = response.content.strip()  # Store for verification
                responses[label] = response.content
            except asyncio.TimeoutError:
                process_manager.remove_verification(ctx.author.id)
                await ctx.author.send("Verification timed out. Please try again.")
                return None
        
        # Handle other questions (existing code)
        else:
            await ctx.author.send(embed=EmbedBuilder.info(f"Question {idx} of {len(VERIFICATION_QUESTIONS)}", question))
            
            try:
                response = await bot.wait_for('message', check=lambda m: m.author == ctx.author and m.channel.type == discord.ChannelType.private, timeout=config.timing.verification_timeout_seconds)
                
                if label == "Invitation Proof":
                    responses[label] = response.content if not response.attachments else response.attachments[0].url
                else:
                    responses[label] = response.content
            except asyncio.TimeoutError:
                process_manager.remove_verification(ctx.author.id)
                await ctx.author.send("Verification timed out. Please try again.")
                return None
    
    return responses

async def _confirm_submission(ctx: commands.Context) -> bool:
    await ctx.author.send(embed=EmbedBuilder.info("Confirm Submission", "Reply **'yes'** to confirm or **'no'** to cancel"))
    
    def check_confirmation(m):
        return m.author == ctx.author and m.channel.type == discord.ChannelType.private and m.content.lower() in ['yes', 'no']
    
    try:
        confirmation = await bot.wait_for('message', check=check_confirmation, timeout=config.timing.confirmation_timeout_seconds)
        
        if confirmation.content.lower() != 'yes':
            process_manager.remove_verification(ctx.author.id)
            await ctx.author.send("Verification cancelled.")
            return False
        return True
    except asyncio.TimeoutError:
        process_manager.remove_verification(ctx.author.id)
        await ctx.author.send("Confirmation timed out.")
        return False


async def _submit_verification(ctx: commands.Context, responses: Dict[str, Optional[str]]) -> None:
    submission_embed = discord.Embed(title=f"Verification Submission from {ctx.author.name}", color=discord.Color.blurple())
    submission_embed.set_thumbnail(url=ctx.author.avatar.url if ctx.author.avatar else ctx.author.default_avatar.url)
    
    now = datetime.now(timezone.utc)
    created_at = ctx.author.created_at.replace(tzinfo=timezone.utc) if ctx.author.created_at.tzinfo is None else ctx.author.created_at
    
    if ctx.author.joined_at:
        joined_at = ctx.author.joined_at.replace(tzinfo=timezone.utc) if ctx.author.joined_at.tzinfo is None else ctx.author.joined_at
        submission_embed.add_field(name="Joined Server", value=f"{ctx.author.joined_at.strftime('%Y-%m-%d')} ({(now - joined_at).days} days ago)", inline=True)
    else:
        submission_embed.add_field(name="Joined Server", value="Unknown (member data not cached)", inline=True)
    
    submission_embed.add_field(name="Account Created", value=f"{ctx.author.created_at.strftime('%Y-%m-%d')} ({(now - created_at).days} days ago)", inline=True)
    
    for label, response in responses.items():
        if response is None:
            submission_embed.add_field(name=f"{label}", value="No response", inline=False)
            continue
        
        if label == "Invitation Proof" and response.startswith("http"):
            submission_embed.add_field(name="Invitation Proof", value="See image below", inline=False)
            submission_embed.set_image(url=response)
        else:
            truncated_response = response[:1021] + "..." if len(response) > 1024 else response
            submission_embed.add_field(name=f"{label}", value=truncated_response or "No response", inline=False)
    
    submission_embed.set_footer(text=f"User ID: {ctx.author.id} | Submitted at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    channel = bot.get_channel(config.channels.verification_submission)
    
    if channel is None:
        process_manager.remove_verification(ctx.author.id)
        logging.error("Could not find verification channel")
        await ctx.author.send("An internal error occurred: Submission channel not found. Please contact an admin.")
        return
    
    view = VerificationButtonsView()
    await channel.send(f"<@&{config.roles.verification_ping}> New verification submission!", embed=submission_embed, view=view)
    
    await ctx.author.send(embed=EmbedBuilder.success("Verification Complete!", "Your application has been submitted for review.\nStaff will review your application shortly."))
    
    process_manager.remove_verification(ctx.author.id)


# --- REPORT SYSTEM ---
@bot.command()
@commands.cooldown(1, 300, commands.BucketType.user)
async def report(ctx: commands.Context, member: Optional[discord.Member] = None) -> None:
    if not member:
        await ctx.send("Please mention the user you're reporting.")
        return
    
    if member == ctx.author:
        await ctx.send("You can't report yourself.")
        return
    
    if member.bot:
        await ctx.send("You can't report bots.")
        return
    
    embed = discord.Embed(title="Report System", description="React with the category number:", color=discord.Color.red())
    
    for emoji, cat in REPORT_CATEGORIES.items():
        embed.add_field(name=emoji, value=cat, inline=False)
    
    msg = await ctx.author.send(embed=embed)
    for emoji in REPORT_CATEGORIES.keys():
        await msg.add_reaction(emoji)
    
    try:
        reaction, _ = await bot.wait_for('reaction_add', check=lambda r, u: u == ctx.author and r.message.id == msg.id and str(r.emoji) in REPORT_CATEGORIES, timeout=60)
        category = REPORT_CATEGORIES[str(reaction.emoji)]
    except asyncio.TimeoutError:
        await ctx.author.send("Report timed out.")
        return
    
    await ctx.author.send(f"Please describe the {category} issue:")
    
    try:
        reason_msg = await bot.wait_for('message', check=lambda m: m.author == ctx.author and m.channel.type == discord.ChannelType.private, timeout=config.timing.report_timeout_seconds)
        
        if reason_msg.content.lower() == 'cancel':
            await ctx.author.send("Report cancelled.")
            return
        
        reason = reason_msg.content
    except asyncio.TimeoutError:
        await ctx.author.send("Report timed out.")
        return
    
    report_embed = discord.Embed(title=f"New {category} Report", color=discord.Color.red())
    report_embed.add_field(name="Reported User", value=f"{member.mention} (ID: {member.id})", inline=False)
    report_embed.add_field(name="Reporter", value=f"{ctx.author.mention} (ID: {ctx.author.id})", inline=False)
    report_embed.add_field(name="Reason", value=reason, inline=False)
    report_embed.timestamp = datetime.now(timezone.utc)
    
    reports_channel = bot.get_channel(config.channels.reports)
    if reports_channel:
        await reports_channel.send(embed=report_embed)
    
    await ctx.author.send("Your report has been submitted.")
    logging.info(f"New {category} report: {member} reported by {ctx.author}")


# --- MESSAGE TOGGLE ---
@bot.command()
async def messageson(ctx: commands.Context) -> None:
    global messages_enabled
    
    if messages_enabled:
        await ctx.send("Message reporting is already enabled.")
        return
    
    messages_enabled = True
    send_report_message.start()
    await ctx.send(embed=EmbedBuilder.success("Messages Enabled", "Periodic report reminder messages have been enabled."))


@bot.command()
async def messagesoff(ctx: commands.Context) -> None:
    global messages_enabled
    
    if not messages_enabled:
        await ctx.send("Message reporting is already disabled.")
        return
    
    messages_enabled = False
    send_report_message.stop()
    await ctx.send(embed=EmbedBuilder.info("Messages Disabled", "Periodic report reminder messages have been disabled."))


# --- POLL COMMAND ---
@bot.command()
@commands.has_permissions(manage_messages=True)
async def poll(ctx: commands.Context, duration: int, question: str, *options: str) -> None:
    if len(options) < config.limits.min_poll_options or len(options) > config.limits.max_poll_options:
        await ctx.send(f"Please provide {config.limits.min_poll_options}-{config.limits.max_poll_options} options.")
        return
    
    if duration < config.limits.min_poll_duration or duration > config.limits.max_poll_duration:
        await ctx.send(f"Duration must be {config.limits.min_poll_duration}-{config.limits.max_poll_duration} seconds.")
        return
    
    embed = discord.Embed(title=f"{question}", description="React to vote!", color=discord.Color.blurple())
    
    for idx, option in enumerate(options):
        embed.add_field(name=f"{POLL_EMOJIS[idx]} {option}", value="(0 %)", inline=False)
    embed.set_footer(text=f"Poll ends in {duration}s")
    
    poll_msg = await ctx.send(embed=embed)
    
    for idx in range(len(options)):
        await poll_msg.add_reaction(POLL_EMOJIS[idx])
    
    end_time = time.time() + duration
    
    while time.time() < end_time:
        await asyncio.sleep(15)
        poll_msg, results, total_votes = await _update_poll(poll_msg, question, options, end_time)
    
    await _show_poll_results(poll_msg, question, options, results, total_votes)


async def _update_poll(poll_msg: discord.Message, question: str, options: Tuple[str, ...], end_time: float) -> Tuple[discord.Message, Dict[int, int], int]:
    poll_msg = await poll_msg.channel.fetch_message(poll_msg.id)
    results: Dict[int, int] = {}
    total_votes = 0
    
    for idx, emoji in enumerate(POLL_EMOJIS[:len(options)]):
        for reaction in poll_msg.reactions:
            if str(reaction.emoji) == emoji:
                votes = reaction.count - 1
                results[idx] = votes
                total_votes += votes
    
    embed = discord.Embed(title=f"{question}", description="React to vote!", color=discord.Color.blurple())
    
    for idx, option in enumerate(options):
        votes = results.get(idx, 0)
        pct = (votes / total_votes * 100) if total_votes > 0 else 0
        bar = 'â–ˆ' * int(pct/10) + 'â–‘' * (10-int(pct/10))
        embed.add_field(name=f"{POLL_EMOJIS[idx]} {option}", value=f"{bar} {votes} ({pct:.1f}%)", inline=False)
    
    embed.set_footer(text=f"{int(end_time - time.time())}s remaining")
    await poll_msg.edit(embed=embed)
    
    return poll_msg, results, total_votes


async def _show_poll_results(poll_msg: discord.Message, question: str, options: Tuple[str, ...], results: Dict[int, int], total_votes: int) -> None:
    result_embed = discord.Embed(title=f"Poll Results: {question}", color=discord.Color.green())
    
    for idx, option in enumerate(options):
        votes = results.get(idx, 0)
        pct = (votes / total_votes * 100) if total_votes > 0 else 0
        bar = 'â–ˆ' * int(pct/10) + 'â–‘' * (10-int(pct/10))
        result_embed.add_field(name=f"{POLL_EMOJIS[idx]} {option}", value=f"{bar} {votes} ({pct:.1f}%)", inline=False)
    
    await poll_msg.edit(embed=result_embed)
    await poll_msg.clear_reactions()


# --- SHUTDOWN & STATUS COMMANDS ---
@bot.command()
@commands.has_permissions(administrator=True)
async def shutdown(ctx: commands.Context) -> None:
    if process_manager.is_busy():
        await ctx.send("Cannot shutdown: Users are currently in verification process!")
        return

    await ctx.send("Shutting down bot...")
    logging.info(f"Bot shutdown initiated by {ctx.author}")
    save_all_data()
    reset_temporary_data()
    data_manager.close()
    process_manager.clear_lock_file()
    await bot.close()


@bot.command()
@commands.has_permissions(administrator=True)
async def botstatus(ctx: commands.Context) -> None:
    embed = discord.Embed(title="Bot Status", color=discord.Color.blue())
    embed.add_field(name="Uptime", value=get_uptime(), inline=True)
    embed.add_field(name="Latency", value=f"{round(bot.latency * 1000)}ms", inline=True)
    embed.add_field(name="Active Verifications", value=str(len(process_manager._active_verifications)), inline=True)
    
    if process_manager.is_busy():
        embed.color = discord.Color.orange()
        embed.add_field(name="Status", value="âš ï¸ Bot is busy! Avoid restarting.", inline=False)
    else:
        embed.color = discord.Color.green()
        embed.add_field(name="Status", value="âœ… Bot is idle. Safe to restart.", inline=False)
    
    await ctx.send(embed=embed)


# --- AUDIT LOG ---
@bot.command()
@commands.has_permissions(view_audit_log=True)
async def auditlog(ctx: commands.Context, limit: int = 10) -> None:
    try:
        entries: List[str] = []
        
        async for entry in ctx.guild.audit_logs(limit=limit):
            entries.append(f"**{entry.created_at.strftime('%Y-%m-%d %H:%M:%S')}** | {str(entry.action).split('.')[-1]} | User: {entry.user} | Target: {entry.target} | Reason: {entry.reason or 'None'}")
        
        if not entries:
            await ctx.send("No audit log entries found.")
            return
        
        for chunk in [entries[i:i+10] for i in range(0, len(entries), 10)]:
            await ctx.send("\n".join(chunk))
            
    except discord.Forbidden:
        await ctx.send("I don't have permission to view audit logs.")


# --- HELP COMMAND ---
@bot.command()
async def cmds(ctx: commands.Context) -> None:
    help_text = """**Available Commands:**

**Moderation:**
`!kick @user [reason]` - Kick a user
`!ban @user [reason]` - Ban a user
`!banid <user_id> [reason]` - Ban user by ID
`!softban @user [reason]` - Softban a user
`!mute @user [reason]` - Mute a user
`!unmute @user` - Unmute a user
`!tempmute @user [seconds] [reason]` - Temporarily mute
`!purge [amount]` - Delete messages
`!purgeall` - Delete all messages in channel
`!lock` - Lock current channel
`!unlock` - Unlock current channel
`!slowmode [seconds]` - Set slowmode

**Blacklist System:**
`/blacklist <keyword>` - Add keyword to blacklist
`/unblacklist <keyword>` - Remove keyword from blacklist
`/blacklistlist` - Show all blacklisted keywords
`/blacklistscan` - Scan all members for blacklisted keywords
`/checkprofile [@user]` - Check a user's profile

**Warnings (NEW):**
`/warn @user <reason>` - Warn a member
`/warnings [@user]` - View warnings
`/clearwarnings @user` - Clear warnings

**Tickets (Full Ticket Tool Clone):**
`/panel` - Create a ticket panel (interactive)
`/panels` - List all ticket panels
`/deletepanel <panel_id>` - Delete a panel
`/claim` - Claim the current ticket
`/unclaim` - Release your claim
`/close [reason]` - Close the current ticket
`/transcript` - Generate a transcript
`/tickets` - View all open tickets
`/ticketsettings` - Configure ticket system
`/ticketblacklist @user [reason]` - Block user from tickets
`/ticketunblacklist @user` - Remove user from blacklist
`/add @user` - Add a user to the current ticket
`/remove @user` - Remove a user from the current ticket
`/rename <name>` - Rename the ticket channel
`/move <panel_id>` - Move ticket to a different panel/category
`/note <text>` - Add a private staff note
`/notes` - View all staff notes for this ticket
`/priority <level>` - Set ticket priority (low/normal/high/urgent)
`/reopen <ticket_id>` - Reopen a closed ticket
`/ticketstats` - View ticket statistics for this server
`/dbcleanup` - Scan and remove stale/invalid database entries

**Giveaways (NEW):**
`/giveaway <hours> <winners> <prize>` - Create giveaway
`/endgiveaway <id>` - End a giveaway early

**Leveling (NEW):**
`/level [@user]` - View your level
`/leaderboard` - View server leaderboard

**Rules Management:**
`/updatemosrules` - Fetch and cache MOS rules
`/updatevprprules` - Fetch and cache VPRP rules
`/setmosrules <text>` - Manually set MOS rules
`/setvprprules <text>` - Manually set VPRP rules
`/viewcachedrules` - View cached rules status

**Roles:**
`!addrole @user [role_name]` - Add role to user
`!removerole @user @role` - Remove role from user
`!roleall @role` - Give role to all members

**Verification:**
`!verify` - Start verification
`!verifyuser @user` - Manually verify user
`!securitycheck @user` - Check user security

**Invites:**
`/setupinvites` - Set up invite tracking
`/checkinvites` - View invite status
`/inviteinfo [code]` - View specific invite
`/regenerateinvites` - Regenerate all invites

**Information:**
`!userinfo @user` - User information
`!serverinfo` - Server information
`!membercount` - Member count
`!ping` - Bot latency
`!uptime_cmd` - Bot uptime
`!botstatus` - Check active processes

**Other:**
`!report @user` - Report a user
`!poll [duration] [question] [options...]` - Create poll
`!auditlog [limit]` - View audit logs
`!shutdown` - Gracefully shutdown bot"""
    await ctx.send(help_text)


# --- ERROR HANDLING ---
@bot.event
async def on_command_error(ctx: commands.Context, error: Exception) -> None:
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("You don't have permission to use this command.")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send("Missing required argument.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("Invalid argument provided.")
    elif isinstance(error, commands.CommandOnCooldown):
        await ctx.send(f"Cooldown. Try again in {error.retry_after:.0f} seconds.")
    else:
        logging.error(f'Error: {str(error)}')
        await ctx.send(f"An error occurred: {str(error)}")


# --- TOKEN & STARTUP ---
def read_token_from_file(token_name: str = 'BOT_Token') -> Optional[str]:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    search_paths = [
        os.path.join(script_dir, 'tokens.txt'),
        os.path.join(script_dir, '..', 'tokens.txt'),
        os.path.join(os.getcwd(), 'tokens.txt'),
        'tokens.txt',
    ]
    
    for filepath in search_paths:
        try:
            if os.path.exists(filepath):
                with open(filepath, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith(f"{token_name}="):
                            logging.info(f"Token found in: {os.path.abspath(filepath)}")
                            return line.split('=', 1)[1].strip()
        except Exception:
            continue
    
    return None


# --- MAIN ENTRY POINT ---
TOKEN = os.environ.get('BOT_Token') or read_token_from_file('BOT_Token')

if TOKEN is None:
    print("=" * 50)
    print("ERROR: Bot token not found!")
    print("Create 'tokens.txt' in the SAME folder as this script with:")
    print("BOT_Token=YOUR_BOT_TOKEN_HERE")
    print("=" * 50)
    logging.error("Bot token not found. Please create tokens.txt")
    exit(1)

if __name__ == "__main__":
    print("=" * 50)
    print("MOS GANG BOT V2 - Starting...")
    print("=" * 50)
    try:
        bot.run(TOKEN)
    finally:
        save_all_data()
        process_manager.clear_lock_file()
