import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, ConversationHandler, CallbackContext, MessageHandler, Filters
import telegram
import openai
import psycopg2
import psycopg2, psycopg2.pool
from psycopg2.extras import RealDictCursor
import re
import os, time, json, traceback
from decimal import Decimal
import queue, threading 

# Set up logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# OpenAI API Key set as env var
client = openai.OpenAI()

# Database connection pool
pool = psycopg2.pool.SimpleConnectionPool(1, 20, f"dbname={os.environ['POSTGRES_DB']} user={os.environ['POSTGRES_USER']} password={os.environ['POSTGRES_PASSWORD']} host={os.environ['POSTGRES_HOST']}")

RATE_LIMIT_SECONDS = 60  # Time frame
USER_LIMIT = 5  # Number of requests in time frame

user_dict = {}

class DecimalEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, Decimal):
      return str(obj)
    return json.JSONEncoder.default(self, obj)

class ConnectionFromPool:
    def __enter__(self):
        self.conn = pool.getconn()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        pool.putconn(self.conn)


def rate_limited(user_id):
    now = time.time()
    if user_id in user_dict:
        user_times = user_dict[user_id]
        if len(user_times) >= USER_LIMIT and now - user_times[0] < RATE_LIMIT_SECONDS:
            return True
        else:
            if len(user_times) >= USER_LIMIT:
                user_dict[user_id] = user_times[1:] + [now] # remove the oldest timestamp, add new one
            else:
                user_dict[user_id].append(now) # add new timestamp
    else:
        user_dict[user_id] = [now]  # Add user to the dictionary
    return False

def start(update: Update, context: CallbackContext):
    """Send a message when the command /start is issued."""
    user_message = update.message.text
    polkadot_address = re.search(r'[1-9A-HJ-NP-Za-km-z]{47,48}', user_message)  
    keyboard = [
     [InlineKeyboardButton("🌟 My nominator status", callback_data='nominator_status')], 
     [InlineKeyboardButton("💡 Staking tips", callback_data='staking_tips')],
     [InlineKeyboardButton("❓ General question", callback_data='general_question')],
     [InlineKeyboardButton("🔄 Reset address", callback_data='reset_address')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if polkadot_address:
        polkadot_address = polkadot_address.group(0)
        save_address(update.message.from_user.id, polkadot_address)
        update.message.reply_text(f'Thank you! Your Polkadot address {polkadot_address} has been saved.', reply_markup=reply_markup)
    else:
        update.message.reply_text('Hi! I am your Polkadot staking assistant.', reply_markup=reply_markup)
    return ConversationHandler.END 


def save_address(user_id, polkadot_address):
    """Save Polkadot address to the database."""
    with ConnectionFromPool() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS users (user_id BIGINT PRIMARY KEY, polkadot_address TEXT, thread_id TEXT)")
                cur.execute("SELECT 1 FROM dashboard WHERE address = %s", (polkadot_address,))
                if cur.fetchone() is None:
                    polkadot_address = None
                cur.execute("INSERT INTO users (user_id, polkadot_address) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET polkadot_address = EXCLUDED.polkadot_address", (user_id, polkadot_address))
            conn.commit()
        except Exception as e:
            logger.error(f"Database error: {e}")
            conn.rollback()

def fetch_relevant_data(address):
    result = {}
    dashboard_query = f"SELECT * FROM dashboard WHERE address = '{address}'"
    subscan_query = f"SELECT * FROM subscan ORDER BY timestamp DESC LIMIT 1"
    
    with ConnectionFromPool() as conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(dashboard_query)
                result['dashboard'] = cur.fetchone()
                if result['dashboard'] is None:
                    result['dashboard'] = {}
                else:
                    # replace None with 0 and ensure numerical values are cast correctly
                    for key, value in result['dashboard'].items():
                        if value is None:
                            result['dashboard'][key] = 0
                        elif isinstance(value, str) and value.isdigit():
                            result['dashboard'][key] = int(value)

                cur.execute(subscan_query)
                result['subscan'] = cur.fetchone()
                if result['subscan'] is None:
                    result['subscan'] = {}
                else:
                    minimum_active_stake = Decimal(result['subscan']['data']['minimumActiveStake']) / 10**10
                    my_stake = Decimal(result['dashboard'].get('currentStake', 0))
                    if my_stake < minimum_active_stake:
                        top_query = f"""
                        SELECT *
                        FROM dashboard
                        WHERE CAST("currentStake" AS NUMERIC) >= {minimum_active_stake * Decimal(1.2)} AND "activeNominator" = '1'
                        ORDER BY CAST("currentStake" AS NUMERIC) ASC
                        LIMIT 3;
                        """
                    else:
                        top_query = f"""
                        SELECT *
                        FROM dashboard
                        WHERE CAST("currentStake" AS NUMERIC) >= {my_stake * Decimal(1.2)} AND "activeNominator" = '1'
                        ORDER BY CAST("currentStake" AS NUMERIC) ASC
                        LIMIT 3;
                        """
                    logger.info(f"top queries: {top_query}")
                    cur.execute(top_query)
                    result['top'] = cur.fetchall()
                    if result['top'] is None:
                        result['top'] = []
                    else:
                        # replace None with 0 and ensure numerical values are cast correctly
                        for top in result['top']:
                            for key, value in top.items():
                                if value is None:
                                    top[key] = 0
                                elif isinstance(value, str) and value.isdigit():
                                    top[key] = int(value)

            conn.commit()

        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
    result = json.loads(json.dumps(result, cls=DecimalEncoder))
    logger.info(f"result: {result}")
    return result

def response_format(message):
    return re.sub(r'^\s*-', '•', str(message), flags=re.MULTILINE).replace("###", "🟦").replace("##", "🟪").replace("#", "🟥")

def update_message(context, chat_id, message_id, message):
    try:
        context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=response_format(message), parse_mode=telegram.ParseMode.MARKDOWN)
        return message
    except telegram.error.RetryAfter as e:
        time.sleep(5)
        update_message(context, chat_id, message_id, message)
    except Exception as e:
        if "Message is not modified" in str(e) or "Message text is empty" in str(e):
            pass
        else:
            logger.error(f"Telegram API error: {e}")
            logger.error(f"Message: {message}")
            traceback.print_exc()

def download_message(thread_id, _queue):
    message = ""
    analyzing_message = "Analyzing data"
    dots = 0
    while True:
        try:
            with client.beta.threads.runs.stream(
                thread_id=thread_id,
                assistant_id=os.environ['OPENAI_ASSISTANT_ID'],
            ) as stream:
                for event in stream:
                    if event.event == "thread.message.delta" and event.data.delta.content:
                        message += event.data.delta.content[0].text.value
                        _queue.put(message)
                    elif event.event == "thread.run.requires_action":
                        address = json.loads(event.data.required_action.submit_tool_outputs.tool_calls[0].function.arguments)['address']
                        result = fetch_relevant_data(address)
                        client.beta.threads.runs.submit_tool_outputs(
                            thread_id=thread_id,
                            run_id=event.data.id,
                            tool_outputs=[
                                {
                                    "tool_call_id": event.data.required_action.submit_tool_outputs.tool_calls[0].id,
                                    "output": json.dumps(result, cls=DecimalEncoder)
                                },
                            ])
                    elif event.event == "thread.message.completed":
                        _queue.put("$$END$$" + message)
                        return
                    elif event.event == "thread.run.failed":
                        if event.last_error.code == "rate_limit_exceeded":
                            logger.error(f"OpenAI Rate limit exceeded: {event.last_error}")
                            message = "Rate limit exceeded. Please wait a moment"
                            for i in range(10):
                                dots = (dots + 1) % 6
                                _queue.put(f"{message}{'.' * dots}")
                                time.sleep(1)
                            _queue.put(message)
                            break
                        else:
                            logger.error(f"thread.run.failed in download_message: {event.last_error}")
                            return  
                    else:
                        logger.info(f"Unknown event: {event.event}")
        except Exception as e:
            logger.error(f"Error in download_message: {e}")
            traceback.print_exc()
            return


def ask_openai(prompt, user_id, context, message_id, chat_id):
    """Ask a question to OpenAI."""
    if rate_limited(user_id):
        context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="You are sending too many requests. Please wait a moment.")
        return "RATELIMIT"

    thread_id = None
    polkadot_address = None
    try:
        with ConnectionFromPool() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT thread_id, polkadot_address FROM users WHERE user_id = %s", (user_id,))
                res = cur.fetchone()
                if res:
                    thread_id = res.get('thread_id')
                    polkadot_address = res.get('polkadot_address', "NOADDRESS")
                    if not thread_id:
                        thread = client.beta.threads.create()
                        thread_id = thread.id
                        cur.execute("UPDATE users SET thread_id = %s WHERE user_id = %s", (thread_id, user_id))
                else:
                    thread = client.beta.threads.create()
                    thread_id = thread.id
                    cur.execute("INSERT INTO users (user_id, thread_id) VALUES (%s, %s)", (user_id, thread_id))
                conn.commit()

        # Check for active runs and stop them if necessary
        active_runs = client.beta.threads.runs.list(thread_id=thread_id)
        for run in active_runs:
            if run.status == "active":
                client.beta.threads.runs.cancel(thread_id=thread_id, run_id=run.id)

        formatted_prompt = f"Polkadot address: {polkadot_address}\n\n{prompt}" 
        client.beta.threads.messages.create(thread_id=thread_id, role="user", content=formatted_prompt)

        _queue = queue.LifoQueue()
        download_thread = threading.Thread(target=download_message, args=(thread_id, _queue))
        download_thread.start()
        
        while True:
            message = _queue.get()
            stop = False
            if "$$END$$" in message:
                message = message.replace("$$END$$", "")
                stop = True
            message = update_message(context, chat_id, message_id, message)
            time.sleep(1)
            if stop:
                break
        return message
    except Exception as e:
        logger.error(f"OpenAI API error: {e}")
        traceback.print_exc()
        return "I'm having trouble thinking of an answer right now."

def reply(update, context):
    user_id = None
    user_message = None
    chat_id = None
    if update.callback_query:
        update.callback_query.answer()
        user_id = update.callback_query.from_user.id
        user_message = "I want to ask a general question!"
        chat_id = update.callback_query.message.chat_id
    else:
        user_id = update.message.from_user.id
        user_message = update.message.text
        chat_id = update.message.chat_id

    log_message(user_id, user_message, author='user')

    #try to parse address
    polkadot_address = re.search(r'[1-9A-HJ-NP-Za-km-z]{47,48}', user_message)
    if polkadot_address:
        polkadot_address = polkadot_address.group(0)
        if not save_address(user_id, polkadot_address):
            user_message = "My address is invalid, tell me about it."
        else:
            user_message += "\nTell me about my stats."
    
    msg = context.bot.send_message(chat_id=chat_id, text="...") 
    
    # Get response from OpenAI
    bot_response = ask_openai(user_message, user_id, context, msg.message_id, chat_id)
    log_message(user_id, bot_response, author='bot')

def log_message(user_id, message, author='bot'):
    """Log messages to the database."""
    with ConnectionFromPool() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS messages (id SERIAL PRIMARY KEY, user_id BIGINT, message TEXT, author TEXT DEFAULT 'bot', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                cur.execute("INSERT INTO messages (user_id, message, author) VALUES (%s, %s, %s)", (user_id, message, author))
            conn.commit()
        except Exception as e:
            logger.error(f"Database error: {e}")
            conn.rollback()

def error(update: Update, context: CallbackContext):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, context.error)


def button_handler(update: Update, context: CallbackContext):
    # if nominator_status, get data from DB, show it.
    # if staking_tips, get data from DB, show it.
    # if general_question, ask for user input, then ConversationHandler.END

    query = update.callback_query
    query.answer()
    try:
        with ConnectionFromPool() as conn:
            if query.data == 'nominator_status':
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT polkadot_address FROM users WHERE user_id = %s", (query.from_user.id,))
                    res = cur.fetchone()
                    if res:
                        polkadot_address = res.get('polkadot_address')
                        if polkadot_address:
                            result = fetch_relevant_data(polkadot_address)
                            template = f"""
🌟 **My Staking Performance** 🌟
📍 **Address:** `{result['dashboard']['address']}`
💰 **Last Era Reward:** `{result['dashboard']['lastEraReward']} DOT`
📈 **APY:** `{result['dashboard']['APY']}%`
🔒 **Current Stake:** `{result['dashboard']['currentStake']} DOT`
                            """
                            context.bot.send_message(chat_id=query.message.chat_id, text=template, parse_mode=telegram.ParseMode.MARKDOWN)
                        else:
                            context.bot.send_message(chat_id=query.message.chat_id, text='Please provide your Polkadot address.')
                    else:
                        context.bot.send_message(chat_id=query.message.chat_id, text='Please provide your Polkadot address.')
            elif query.data == 'staking_tips':
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT polkadot_address FROM users WHERE user_id = %s", (query.from_user.id,))
                    res = cur.fetchone()
                    if res:
                        polkadot_address = res.get('polkadot_address')
                        if polkadot_address:
                            result = fetch_relevant_data(polkadot_address)
                            active_status = "🟥 You are not active." if not result["dashboard"]["activeNominator"] == 1 else "🟩 You are active."
                            tips = set()
                            if result["top"]:
                                top = max(result["top"], key=lambda x: (float(x["currentStake"]), float(x.get("currentEraFee", 0))))

                                top_currentStake = float(top["currentStake"])
                                top_activeValidators = int(top["activeValidators"])
                                top_fee = float(top.get("currentEraFee", 0))

                                dashboard_currentStake = float(result["dashboard"]["currentStake"])
                                dashboard_activeValidators = int(result["dashboard"]["activeValidators"])
                                dashboard_fee = float(result["dashboard"].get("currentEraFee", 0))

                                if top_currentStake > dashboard_currentStake:
                                    tips.add(f"📈 **Increase your stake** to at least `{top_currentStake} DOT` to match top performers.")
                                if top_activeValidators > dashboard_activeValidators:
                                    tips.add(f"👥 **Increase your number of active validators** to at least `{top_activeValidators}` to match top performers.")
                                if top_fee < dashboard_fee:
                                    tips.add(f"💸 **Reduce your fee** to at most `{top_fee}` to match top performers.")
                                if dashboard_currentStake < result["subscan"]["data"]["minimumActiveStake"]: 
                                    tips.add(f"🔒 **Increase your stake** to at least `{int(result['subscan']['data']['minimumActiveStake']) / 10**10} DOT` to meet the minimum active stake.")
                                if dashboard_activeValidators < 1:
                                    tips.add("🔧 **Ensure you have at least one active validator.**")
                                    
                            message = f"{active_status}\n\n"
                            if tips:
                                message += "Here are some tips to improve your performance:\n"
                                for tip in tips:
                                    message += f"\n- {tip}"
                            else:
                                message += "No tips available."
                            
                            context.bot.send_message(chat_id=query.message.chat_id, text=message, parse_mode=telegram.ParseMode.MARKDOWN)
                        else:
                            context.bot.send_message(chat_id=query.message.chat_id, text='Please provide your Polkadot address.')
                    else:
                        context.bot.send_message(chat_id=query.message.chat_id, text='Please provide your Polkadot address.')
            elif query.data == 'general_question':
                reply(update, context)
            elif query.data == 'reset_address':
                context.bot.send_message(chat_id=query.message.chat_id, text='Send me your new address.')
        return ConversationHandler.END 
    except Exception as e:
        logger.error(f"Button handler error: {e}")
        traceback.print_exc()

def provision_assistant():
    with open('fetch_relevant_data.jsonl', 'r') as file:
        updated_assistant = client.beta.assistants.update(
            os.environ['OPENAI_ASSISTANT_ID'],
            tools=[
                {"type": "function", "function": json.load(file)}
            ],
            model="gpt-4o",
            instructions="""
You are a telegram bot. You speak to a polkadot nominator/bot user. You know user's polkadot address. Compare user to top performers in his category, refer to them by their address, suggest tips for improvement if relevant. Don't print out raw data, give advice in words, while backing it up with numbers where relevant. DON'T answer irrelevant questions. be polite, BRIEF, ask what they want.

If you need to call fetch_relevant_data and the address is NOADDRESS, ask user for address.

You operate on nominator data, you can't recommend validators as you don't know validator's addresses and you can't access validator's data.

Prefer to structure your answer using headings ###, ## and # instead of big paragraphs or numbered lists.

Instead of refering to polkadot js apps and subscan, refer to https://app.motif.network/ - this is the service YOU represent, which allows you to monitor staking performance on polkadot. 

Refer them to these links when relevant:

1. "Why am I not Getting Staking Rewards?" - Troubleshooting guide for missing staking rewards.(https://support.polkadot.network/support/solutions/articles/65000170805-why-am-i-not-getting-staking-rewards-)
2. "Polkadot Staking Site not Detecting Enkrypt Wallet" - Assistance for resolving issues with Enkrypt wallet detection on the staking site. (https://forum.polkadot.network/t/polkadot-staking-site-not-detecting-enkrypt-wallet/7064#:~:text=Polkadot%20Staking%20Site%20not%20Detecting%20Enkrypt%20Wallet)
3. "My Password is not working" - Steps to address password authentication problems. (https://support.polkadot.network/support/solutions/articles/65000170268-my-password-is-not-working)
4. "How to Troubleshoot Connection Issues" - Guide to troubleshooting connection problems with Polkadot JS UI.(https://support.polkadot.network/support/solutions/articles/65000176918-polkadot-js-ui-how-to-troubleshoot-connection-issues)
5. "I Withdrew BDOT instead of DOT by Mistake" - Instructions for rectifying accidental withdrawal of BDOT instead of DOT. (https://support.polkadot.network/support/solutions/articles/65000173537-i-withdrew-bdot-instead-of-dot-by-mistake)
6. "I Sent Funds to a Validator instead of Staking Them!" - Help for users who mistakenly sent funds to a validator instead of staking. (https://support.polkadot.network/support/solutions/articles/65000167146-i-sent-funds-to-a-validator-instead-of-staking-them-)
7. "Staking FAQ's" - Frequently asked questions and answers regarding staking on Polkadot. (https://support.polkadot.network/support/solutions/articles/65000181959-staking-faq-s)
8. "Staking Dashboard no longer connecting to JS wallet accounts" - Resolution steps for issues with the staking dashboard connecting to JS wallet accounts. (https://github.com/paritytech/polkadot-staking-dashboard/issues/2058)
9. "Validators doesn't stay favorited" - Addressing the problem of favorite validators not persisting. (https://github.com/paritytech/polkadot-staking-dashboard/issues/2015)
10. "Staking page not fully loading" - Troubleshooting steps for when the staking page fails to load completely. (https://github.com/paritytech/polkadot-staking-dashboard/issues/1212)
11. "I can't unstake my DOT" - Assistance for users encountering difficulties with unstaking DOT.(https://forum.polkadot.network/t/i-cant-unstake-my-dot-please-help-me/4414)
            """
        )
        return updated_assistant


def main():
    provision_assistant()
    # Create the Updater and pass it your bot's token.
    updater = Updater(os.environ['TELEGRAM_BOT_TOKEN'], use_context=True)

    # Get the dispatcher to register handlers
    dp = updater.dispatcher

    dp.add_handler(CommandHandler('start', start))
    dp.add_handler(CallbackQueryHandler(button_handler))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, reply))

    # log all errors
    dp.add_error_handler(error)

    # Start the Bot
    updater.start_polling()
    
    while True:
        time.sleep(60) #instead of updater.idle()

if __name__ == '__main__':
    main()


