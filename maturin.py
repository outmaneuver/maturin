import os

import discord
from discord import app_commands
from discord.utils import get
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
PERSONAL = int(os.getenv("PERSONAL_SERVER"))
HSKUCW = int(os.getenv("HSKUCW"))
DIADO = int(os.getenv("DIADO"))

LETTER_CHANNEL = os.getenv("LETTER_CHANNEL")


intents = discord.Intents.default()
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)


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
    name="testing", description="Testing Commands", guild_ids=[PERSONAL, HSKUCW]
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


@diplo.command(
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


@diplo.command(
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
    letter_channel_id = None
    thread_id = None
    # check to make sure that a letter channel exists
    for channel in interaction.guild.channels:
        if channel.name == LETTER_CHANNEL:
            letter_channel_id = channel.id

    if letter_channel_id is None:
        raise ValueError
    letter_channel = interaction.guild.get_channel(int(letter_channel_id))

    # build the recipient letter thread name
    if isinstance(recipient, discord.Member):
        thread_name = f"{recipient.nick} Personal Letters"
    elif isinstance(recipient, discord.Role):
        thread_name = f"{recipient.name} Letters"

    # check to see if the thread already exists
    for thread in letter_channel.threads:
        if thread.name == thread_name:
            thread_id = thread.id

    # if thread does not exist create thread
    if thread_id is None:
        thread = await letter_channel.create_thread(
            name=thread_name,
            message=None,
            invitable=False,
            # slowmode_delay=21600,
        )
        await thread.send(f"{u_role.mention} {s_role.mention} {recipient.mention}")

    elif thread_id is not None:
        thread = interaction.guild.get_thread(int(thread_id))

    # send the message to the correct letter channel
    adj_message = f"Letter from {interaction.user.nick}: \n```{message}```"
    await thread.send(adj_message)

    await interaction.response.send_message(
        f"Sent letter to {recipient.nick}",
        ephemeral=True,
    )

    # TODO - make player names without out nicknames show up as their discord names instead of none
    # TODO - copy the sent letters to the player's channel
    # TODO - add timegates to prevent letters
    # TODO - add storage to store a thread id per player and role vs using the name lookup
    # TODO - tweak formatting - see if we can get a good template


tree.add_command(diplo)
tree.add_command(testing)

client.run(TOKEN)
