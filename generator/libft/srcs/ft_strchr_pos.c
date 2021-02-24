/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_strchr_pos.c                                    :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/10 13:02:41 by sikpenou          #+#    #+#             */
/*   Updated: 2019/10/03 17:54:41 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

int		ft_strchr_pos(char *s, int c)
{
	char	*str;

	str = s;
	while (*str ^ c && *str)
	{
		if (!(*++str ^ c) || !*str)
			return (!*str ? -1 : str - s);
		else if (!(*++str ^ c) || !*str)
			return (!*str ? -1 : str - s);
		else if (!(*++str ^ c) || !*str)
			return (!*str ? -1 : str - s);
		else if (!(*++str ^ c) || !*str)
			return (!*str ? -1 : str - s);
		else if (!(*++str ^ c) || !*str)
			return (!*str ? -1 : str - s);
		else if (!(*++str ^ c) || !*str)
			return (!*str ? -1 : str - s);
		else if (!(*++str ^ c) || !*str)
			return (!*str ? -1 : str - s);
		else if (!(*++str ^ c) || !*str)
			return (!*str ? -1 : str - s);
		else if (!(*++str ^ c) || !*str)
			return (!*str ? -1 : str - s);
	}
	return (!*str ? -1 : str - s);
}
