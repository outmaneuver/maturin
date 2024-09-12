import os

import discord
import pandas as pd
from discord import app_commands
from discord.utils import get
from dotenv import load_dotenv
from datetime import datetime

from util import database

load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
PERSONAL = int(os.getenv("PERSONAL_SERVER"))
HSKUCW = int(os.getenv("HSKUCW"))
DIADO = int(os.getenv("DIADO"))

LETTER_CHANNEL = os.getenv("LETTER_CHANNEL")


intents = discord.Intents.default()
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

database.initialize()


@client.event
async def on_ready():
    await tree.sync(guild=discord.Object(id=PERSONAL))
    print(f"We have logged in as {client.user}")


diplo = app_commands.Group(
    name="diplo",
    description="Diplomacy Commands",
    guild_ids=[PERSONAL, DIADO, HSKUCW],
)
testing = app_commands.Group(
    name="testing", description="Testing Commands", guild_ids=[PERSONAL]
)


@testing.command(
    name="test_maturin",
    description="Test command that will parrot things back at you",
)
async def test_maturin(interaction, message: str = None):
    await interaction.response.send_message(f"Hello! I am a robot! You said {message}")


@testing.command(
    name="sync_maturin",
    description="will sync commands with servers",
)
async def sync_maturin(interaction, server: str):
    await tree.sync(guild=discord.Object(id=int(server)))
    await interaction.response.send_message(
        f"Commands Synced with {server} Successfully!"
    )


@testing.command(
    name="personal_letter",
    description="create a private thread with a user",
)
@app_commands.describe(user="the user you would like to send the letter to")
async def personal_letter(
    interaction: discord.Interaction, user: discord.Member | discord.Role
):
    channel = client.get_channel(interaction.channel.id)
    u_role = get(interaction.guild.roles, name="Diplo Umpire")
    s_role = get(interaction.guild.roles, name="Spectator")

    thread = await channel.create_thread(
        name=f"PL: {interaction.user.nick} - {user.nick}",
        message=None,
        invitable=False,
        slowmode_delay=21600,
    )
    await thread.send(
        f"Personal letter thread between <@{interaction.user.id}> and with {user.mention}. {u_role.mention} {s_role.mention}"
    )

    await interaction.response.send_message(
        f"Created - Personal Letters: {interaction.user.name} - {user.name}",
        ephemeral=True,
    )


@testing.command(
    name="diplomatic_communications",
    description="create a thread for diplomatic communications between states",
    # guild=discord.Object(id=PERSONAL),
)
@app_commands.describe(
    your_party="the country role of your party",
    other_party="the country role of the other party you want to talk to",
)
async def state_letter(
    interaction: discord.Interaction,
    your_party: discord.Role,
    other_party: discord.Role,
):
    channel = client.get_channel(interaction.channel.id)
    u_role = get(interaction.guild.roles, name="Diplo Umpire")
    s_role = get(interaction.guild.roles, name="Spectator")
    thread = await channel.create_thread(
        name=f"D: {your_party.name} - {other_party.name}",
        message=None,
        invitable=False,
        slowmode_delay=21600,
    )
    await thread.send(
        f"Diplomacy thread between {your_party.mention} and with {other_party.mention}. {u_role.mention} {s_role.mention}"
    )

    await interaction.response.send_message(
        f"Created that thread - Diplomacy: {your_party.name} - {other_party.name}",
        ephemeral=True,
    )


@diplo.command(
    name="send_letter",
    description="send a letter to another player or state inbox",
)
@app_commands.describe(
    recipient="the person or state you are sending the letter too",
    message="the content of your letter",
)
async def send_letter(
    interaction: discord.Interaction,
    recipient: discord.Role | discord.Member,
    message: str,
):
    u_role = get(interaction.guild.roles, name="Diplo Umpire")
    s_role = get(interaction.guild.roles, name="Spectator")
    d_role = get(interaction.guild.roles, name="Diplomat")
    b_role = get(interaction.guild.roles, name="Banker")
    n_role = get(interaction.guild.roles, name="Newspaper Writer")
    now_stamp = int(datetime.now().timestamp())

    max_letter_size = 1900
    if len(message) > max_letter_size:
        await interaction.response.send_message(
            f"Sorry, your postal system can only handle messages less than {max_letter_size} at this time.",
            ephemeral=True,
        )
        return

    # letter channel is the base channel that all the threads will be under.
    letter_channel_id = None
    # check to make sure that a letter channel exists
    for channel in interaction.guild.channels:
        if channel.name == LETTER_CHANNEL:
            letter_channel_id = channel.id

    if letter_channel_id is None:
        raise ValueError
    letter_channel = interaction.guild.get_channel(int(letter_channel_id))

    if isinstance(recipient, discord.Role):
        gp = 14400
        chk = database.check_message_time(
            interaction.user.top_role.id, recipient.id, now_stamp, gp
        )
    elif isinstance(recipient, discord.Member):
        if d_role in interaction.user.roles or b_role in interaction.user.roles:
            gp = 3600
        else:
            gp = 86400
        chk = database.check_message_time(
            interaction.user.id, recipient.id, now_stamp, gp
        )
    else:
        chk = None

    if chk is not None:
        if isinstance(recipient, discord.Role):
            nm = recipient.name
        elif recipient.nick is None:
            nm = recipient.name
        else:
            nm = recipient.nick

        await interaction.response.send_message(
            f"Oh no! The mailman for {nm} has left already! They will be back in <t:{chk}:R>",
            ephemeral=True,
        )
        return

    # TODO this needs to be absracted when im not in a rush, for now, icky if statement
    ### PLAYERS WORKFLOW
    if isinstance(recipient, discord.Member):
        # check for sender data
        udf = database.user_lookup(str(interaction.user.id))
        if udf.shape[0] == 0:
            # make new user
            database.create_user(
                interaction.user.id, interaction.user.name, interaction.user.nick
            )
            udf = database.user_lookup(str(interaction.user.id))
        elif udf.shape[0] > 1:
            raise ValueError("unique constraint broken")

        udf = udf.iloc[0].to_dict()

        # look for thread
        uth = database.get_user_inbox(str(interaction.user.id))
        uth = uth.iloc[0].to_dict()

        thread = letter_channel.get_thread(int(uth["personal_inbox_id"]))
        if thread is None:
            # make new thread
            if udf["nick"] == "None":
                thread_name = f"{udf['name']} Personal Letters"
            else:
                thread_name = f"{udf['nick']} Personal Letters"

            # if thread does not exist create thread

            thread = await letter_channel.create_thread(
                name=thread_name,
                message=None,
                invitable=False,
            )
            await thread.send(
                f"{u_role.mention} {s_role.mention} {interaction.user.mention}"
            )

            # save thread - TODO needs to overwrite if a bad thread exists
            try:
                database.create_user_inbox(
                    str(udf["user_id"]), str(thread.id), thread.name
                )
            except:
                database.update_user_inbox(
                    str(udf["user_id"]), str(thread.id), thread.name
                )
            uth = {
                "user_id": str(interaction.user.id),
                "personal_inbox_id": str(thread.id),
                "personal_inbox_name": thread.name,
            }

        # resolve recipient name
        if recipient.nick is None:
            recp_name = recipient.name
        else:
            recp_name = recipient.nick

        # send letter to sender thread
        adj_message = (
            f"Sent letter to **{recp_name}**: \n```{message}```\nAt <t:{now_stamp}:f>"
        )
        await thread.send(adj_message)

        # make sure recipient has thread
        rdf = database.user_lookup(str(recipient.id))
        if rdf.shape[0] == 0:
            # make new user
            database.create_user(recipient.id, recipient.name, recipient.nick)
            rdf = database.user_lookup(str(recipient.id))
        elif rdf.shape[0] > 1:
            raise ValueError("unqiue constraint broken")

        rdf = rdf.iloc[0].to_dict()
        # look for thread
        rth = database.get_user_inbox(str(recipient.id))
        rth = rth.iloc[0].to_dict()

        thread = letter_channel.get_thread(int(rth["personal_inbox_id"]))
        if thread is None:
            # make new thread
            # build the recipient letter thread name
            if isinstance(recipient, discord.Member):
                thread_name = f"{recp_name} Personal Letters"
            elif isinstance(recipient, discord.Role):
                thread_name = f"{recp_name} Letters"

            # if thread does not exist create thread

            thread = await letter_channel.create_thread(
                name=thread_name,
                message=None,
                invitable=False,
            )
            await thread.send(f"{u_role.mention} {s_role.mention} {recipient.mention}")

            # save thread
            try:
                database.create_user_inbox(
                    str(rdf["user_id"]), str(thread.id), thread.name
                )
            except:
                database.update_user_inbox(
                    str(rdf["user_id"]), str(thread.id), thread.name
                )
            rth = {
                "user_id": str(rdf["user_id"]),
                "personal_inbox_id": str(thread.id),
                "personal_inbox_name": thread.name,
            }

        # resolve sender name
        if interaction.user.nick is None:
            sender_name = interaction.user.name
        else:
            sender_name = interaction.user.nick

        # send letter to recipient thread

        adj_message = (
            f"Letter from **{sender_name}**: \n```{message}```\nAt <t:{now_stamp}:f>"
        )
        await thread.send(adj_message)

        # save message to message table
        database.create_message(udf["user_id"], rdf["user_id"], now_stamp, message)

        await interaction.response.send_message(
            f"Sent letter to **{recp_name}**, next in <t:{now_stamp + gp}:R>",
            ephemeral=True,
        )

    ### ROLES WORKFLOW
    elif isinstance(recipient, discord.Role):
        # check for sender data
        udf = database.role_lookup(str(interaction.user.top_role.id))
        if udf.shape[0] == 0:
            # make new user
            database.create_role(
                interaction.user.top_role.id,
                interaction.user.top_role.name,
            )
            udf = database.role_lookup(str(interaction.user.top_role.id))
        elif udf.shape[0] > 1:
            raise ValueError("unique constraint broken")

        udf = udf.iloc[0].to_dict()

        # look for thread
        uth = database.get_user_inbox(str(interaction.user.top_role.id))
        if uth.shape[0] == 0:
            # make new thread
            thread_name = f"{udf['name']} State Letters"

            ## if it's being sent to a newspaper writer, don't do it
            if recipient != n_role:

                thread = await letter_channel.create_thread(
                    name=thread_name,
                    message=None,
                    invitable=False,
                )
                await thread.send(
                    f"{u_role.mention} {s_role.mention} {interaction.user.top_role.mention}"
                )

                # save thread
                database.create_user_inbox(
                    str(udf["role_id"]), str(thread.id), thread.name
                )
                uth = {
                    "role_id": str(interaction.user.top_role.id),
                    "personal_inbox_id": str(thread.id),
                    "personal_inbox_name": thread.name,
                }

        if isinstance(uth, pd.DataFrame):
            uth = uth.iloc[0].to_dict()

        # resolve recipient name
        recp_name = recipient.name

        if interaction.user.nick is None:
            s_n = interaction.user.name
        else:
            s_n = interaction.user.nick

        ## if it's being sent to a newspaper writer, don't do it
        if recipient != n_role:

            # send letter to sender thread
            thread = letter_channel.get_thread(int(uth["personal_inbox_id"]))
            adj_message = f"**{s_n.title()}** sent state letter to **{recp_name}**: \n```{message}```\nAt <t:{now_stamp}:f>"
            await thread.send(adj_message)

        # make sure recipient has thread
        rdf = database.role_lookup(str(recipient.id))
        if rdf.shape[0] == 0:
            # make new user
            database.create_role(recipient.id, recipient.name)
            rdf = database.role_lookup(str(recipient.id))
        elif rdf.shape[0] > 1:
            raise ValueError("unqiue constraint broken")

        rdf = rdf.iloc[0].to_dict()
        # look for thread
        rth = database.get_user_inbox(str(recipient.id))
        if rth.shape[0] == 0:
            # make new thread
            # build the recipient letter thread name
            thread_name = f"{recp_name} State Letters"

            # if thread does not exist create thread

            thread = await letter_channel.create_thread(
                name=thread_name,
                message=None,
                invitable=False,
            )
            await thread.send(f"{u_role.mention} {s_role.mention} {recipient.mention}")

            # save thread
            database.create_user_inbox(str(rdf["role_id"]), str(thread.id), thread.name)
            rth = {
                "role_id": str(rdf["role_id"]),
                "personal_inbox_id": str(thread.id),
                "personal_inbox_name": thread.name,
            }

        if isinstance(rth, pd.DataFrame):
            rth = rth.iloc[0].to_dict()

        # resolve sender name
        sender_name = interaction.user.top_role.name

        # send letter to recipient thread
        thread = letter_channel.get_thread(int(rth["personal_inbox_id"]))
        adj_message = (
            f"Letter from **{sender_name}**: \n```{message}```\nAt <t:{now_stamp}:f>"
        )
        await thread.send(adj_message)

        # save message to message table
        database.create_message(udf["role_id"], rdf["role_id"], now_stamp, message)

        await interaction.response.send_message(
            f"Sent letter to **{recp_name}**, next in <t:{now_stamp + gp}:R>",
            ephemeral=True,
        )


tree.add_command(diplo)
tree.add_command(testing)

client.run(TOKEN)
