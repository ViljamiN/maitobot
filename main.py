import os
import logging
import psycopg2
from datetime import datetime
from telegram import KeyboardButton, ReplyKeyboardMarkup, Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def check_milk_status():
    try:
        DATABASE_URL = os.environ.get('DATABASE_URL')
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        cursor = conn.cursor()
        cursor.execute("SELECT id, amount, expiration_date FROM milk WHERE amount > 0 ORDER BY expiration_date")
        rows = cursor.fetchall()
        if rows:
            return [(milk_id, remaining_amount, expiration_date) for milk_id, remaining_amount, expiration_date in rows]
        else:
            return []
    except psycopg2.Error as e:
        logging.error("Error checking milk status:", e)
        return []
    finally:
        cursor.close()
        conn.close()

def add_milk(buyer_name, amount, expiration_date):
    try:
        DATABASE_URL = os.environ.get('DATABASE_URL')
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        cursor = conn.cursor()
        cursor.execute("INSERT INTO milk (amount, expiration_date) VALUES (%s, %s) RETURNING id", (amount, expiration_date))
        milk_id = cursor.fetchone()[0]
        cursor.execute("INSERT INTO purchases (buyer_name, amount, milk_id) VALUES (%s, %s, %s)", (buyer_name, amount, milk_id))
        conn.commit()
    except psycopg2.Error as e:
        logging.error("Error adding milk:", e)
    finally:
        cursor.close()
        conn.close()

def drink_milk(drinker_name, amount):
    try:
        DATABASE_URL = os.environ.get('DATABASE_URL')
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM milk WHERE amount > 0 ORDER BY expiration_date LIMIT 1 FOR UPDATE")
        milk_id = cursor.fetchone()
        cursor.execute("INSERT INTO consumption (drinker_name, amount, milk_id) VALUES (%s, %s, %s)", (drinker_name, amount, milk_id))
        conn.commit()
    except psycopg2.Error as e:
        logging.error("Error drinking milk:", e)
    finally:
        cursor.close()
        conn.close()

def empty_milk(milk_id):
    try:
        DATABASE_URL = os.environ.get('DATABASE_URL')
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        cursor = conn.cursor()
        cursor.execute("UPDATE milk SET amount = 0 WHERE id = %s", (milk_id,))
        conn.commit()
    except psycopg2.Error as e:
        logging.error("Error emptying milk:", e)
    finally:
        cursor.close()
        conn.close()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Tervetuloa maitoliittoon!")
    except (IndexError, ValueError, TypeError, KeyError, Exception) as e:
        logging.error(f"Error processing /start command: {e}")

async def help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Komennot:\n/osta <koko litroissa> <vanhentumispäivä (dd.mm.yyyy)>\n/juo (juo maitoa - ei tee muuta)\n/kellota (tyhjentää viimeksi ostetun ei-tyhjennetyn maitotölkin)\n/tilanne (kertoo onko olkkarilla maitoa)\n/leaderboard (top 5 maitofanit ja maitosupportterit)")
    except (IndexError, ValueError, TypeError, KeyError, Exception) as e:
        logging.error(f"Error processing /help command: {e}")

async def buy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        size = float(context.args[0])
        input_date = datetime.strptime(context.args[1], '%d.%m.%Y')
        expiration_date = input_date.strftime('%Y-%m-%d')
        buyer_name = " ".join(filter(None, [update.effective_user.first_name, update.effective_user.last_name]))
        if not buyer_name:
            buyer_name = update.effective_user.username or str(update.effective_user.id)
        add_milk(buyer_name, size, expiration_date)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"Maitoa ostettu!\nOstaja: {buyer_name}\nKoko: {size} litraa\nVanhentumispäivä: {input_date}")
    except ValueError as e:
        logging.error(f"Error processing /osta command: {e}")
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Virheellinen syöte. Käyttö: /osta <koko litroissa> <vanhentumispäivä (dd.mm.yyyy)>")
    except IndexError as e:
        logging.error(f"Error processing /osta command: {e}")
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Puuttuva syöte. Käyttö: /osta <koko litroissa> <vanhentumispäivä (dd.mm.yyyy)>")
    except Exception as e:
        logging.error(f"Error processing /osta command: {e}")

async def drink(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        drinker_name = " ".join(filter(None, [update.effective_user.first_name, update.effective_user.last_name]))
        if not drinker_name:
            drinker_name = update.effective_user.username or str(update.effective_user.id)
        non_empty_milks = check_milk_status()
        if non_empty_milks:
            drink_milk(drinker_name, 1)
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Joit maitoa! Hyvä sinä!")
        else:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Ei siellä pitäisi olla maitoa juotavaksi?!")
    except Exception as e:
        logging.error(f"Error processing /juo command: {e}")

async def empty(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        non_empty_milks = check_milk_status()
        if non_empty_milks:
            keyboard_buttons = [
                [KeyboardButton(f"{milk_id}: {remaining_amount}L - vanhenee {expiration_date.strftime('%d.%m.%Y')}")]
                for milk_id, remaining_amount, expiration_date in non_empty_milks
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard_buttons, resize_keyboard=True, one_time_keyboard=True)
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Valitse tyhjennettävä maito:",
                reply_markup=reply_markup
            )
        else:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Ei siellä pitäisi olla maitoa tyhjennettäväksi!")
    except Exception as e:
        logging.error(f"Error processing /kellota command: {e}")

async def handle_selected_milk(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        selected_milk = update.message.text.split(':')[0].strip()
        empty_milk(selected_milk)
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Maito tyhjennetty!")
    except Exception as e:
        logging.error(f"Error handling selected milk bottle: {e}")

async def tilanne(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        non_empty_milks = check_milk_status()
        if non_empty_milks:
            message = "Maitotilanne:\n"
            message += "\n".join([f"{milk_id}: {remaining_amount}L - vanhenee {expiration_date.strftime('%d.%m.%Y')}" for milk_id, remaining_amount, expiration_date in non_empty_milks])
            await context.bot.send_message(chat_id=update.effective_chat.id, text=message)
        else:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Ei maitoa Olkkarilla!")
    except (IndexError, ValueError, TypeError, KeyError, Exception) as e:
        logging.error(f"Error processing /tilanne command: {e}")

def get_top_drinkers(limit=5):
    try:
        DATABASE_URL = os.environ.get('DATABASE_URL')
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        cursor = conn.cursor()
        cursor.execute("SELECT drinker_name, SUM(amount) FROM consumption GROUP BY drinker_name ORDER BY SUM(amount) DESC LIMIT %s", (limit,))
        top_drinkers = cursor.fetchall()
        return top_drinkers
    except psycopg2.Error as e:
        logging.error("Error getting top drinkers:", e)
        return None

def get_top_buyers(limit=5):
    try:
        DATABASE_URL = os.environ.get('DATABASE_URL')
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        cursor = conn.cursor()
        cursor.execute("SELECT buyer_name, SUM(amount) FROM purchases GROUP BY buyer_name ORDER BY SUM(amount) DESC LIMIT %s", (limit,))
        top_buyers = cursor.fetchall()
        return top_buyers
    except psycopg2.Error as e:
        logging.error("Error getting top buyers:", e)
        return None

def format_leaderboard_message(data, header, drinker_or_buyer):
    if not data:
        return f"{header}Ei tietoja vielä.\n"
    message = f"{header}"
    if drinker_or_buyer == "drinker":
        for i, (name, amount) in enumerate(data, start=1):
            reply = f"{i}. {name}: {amount} kuppia"
            message += reply + "\n"
    else:
        for i, (name, amount) in enumerate(data, start=1):
            reply = f"{i}. {name}: {amount} litraa"
            message += reply + "\n"
    return message

async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        top_drinkers = get_top_drinkers()
        top_buyers = get_top_buyers()

        drinkers_message = format_leaderboard_message(top_drinkers, "Top maitoa juoneet:\n", "drinker")
        buyers_message = format_leaderboard_message(top_buyers, "Top maitoa ostaneet:\n", "buyer")

        leaderboard_message = f"{drinkers_message}\n{buyers_message}"
        await context.bot.send_message(chat_id=update.effective_chat.id, text=leaderboard_message)
    except Exception as e:
        logging.error(f"Error processing /leaderboard command: {e}")

def main() -> None:
    TOKEN = os.environ.get('TOKEN')
    application = ApplicationBuilder().token(TOKEN).build()

    start_handler = CommandHandler('start', start)
    help_handler = CommandHandler('help', help)
    osta_handler = CommandHandler('osta', buy)
    juo_handler = CommandHandler('juo', drink)
    empty_handler = CommandHandler('kellota', empty)
    tilanne_handler = CommandHandler('tilanne', tilanne)
    leaderboard_handler = CommandHandler('leaderboard', leaderboard)
    reply_keyboard_handler = MessageHandler(filters.text & ~filters.command, handle_selected_milk)

    application.add_handler(start_handler)
    application.add_handler(help_handler)
    application.add_handler(osta_handler)
    application.add_handler(juo_handler)
    application.add_handler(empty_handler)
    application.add_handler(tilanne_handler)
    application.add_handler(leaderboard_handler)
    application.add_handler(reply_keyboard_handler)

    application.run_polling()

main()