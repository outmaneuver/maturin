import os

import discord
from discord import app_commands
from discord.utils import get
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")


intents = discord.Intents.default()
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)


@client.event
async def on_ready():
    await tree.sync(guild=discord.Object(id=564698349425655809))
    print(f"We have logged in as {client.user}")


diplo = app_commands.Group(
    name="diplo",
    description="Diplomacy Commands",
    guild_ids=[564698349425655809, 1229818432212566118],
)
testing = app_commands.Group(
    name="testing",
    description="Testing Commands",
    guild_ids=[564698349425655809, 1229818432212566118],
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
async def sync_maturing(interaction, server: str):
    await tree.sync(guild=discord.Object(id=int(server)))
    await interaction.response.send_message(
        f"Commands Synced with {server} Successfully!"
    )


@diplo.command(
    name="personal_letter",
    description="create a private thread with a user",
    # guild=discord.Object(id=564698349425655809),
)
@app_commands.describe(user="the user you would like to send the letter to")
async def personal_letter(
    interaction: discord.Interaction, user: discord.Member | discord.Role
):
    channel = client.get_channel(interaction.channel.id)
    u_role = get(interaction.guild.roles, name="Umpire")
    s_role = get(interaction.guild.roles, name="Spectator")
    thread = await channel.create_thread(
        name=f"Personal Letters: {interaction.user.name} - {user.name}",
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
    # guild=discord.Object(id=564698349425655809),
)
@app_commands.describe(
    your_party="the country role of your party",
    other_party="the country role of the other party you want to talk to",
)
async def personal_letter(
    interaction: discord.Interaction,
    your_party: discord.Role,
    other_party: discord.Role,
):
    channel = client.get_channel(interaction.channel.id)
    u_role = get(interaction.guild.roles, name="Umpire")
    s_role = get(interaction.guild.roles, name="Spectator")
    thread = await channel.create_thread(
        name=f"Diplomacy: {your_party.name} - {other_party.name}",
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


tree.add_command(diplo)
tree.add_command(testing)

client.run(TOKEN)
